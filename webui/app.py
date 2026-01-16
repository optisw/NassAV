import os
import re
import json
import time
import uuid
import shutil
import threading
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent.parent
WEB_DIR = Path(__file__).resolve().parent
STATIC_DIR = WEB_DIR / "static"

APP_STATE_PATH = Path("/tmp/nassav_webui_state.json")

DOWNLOAD_DB_PATH = BASE_DIR / "db" / "downloaded.db"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

TASKS: Dict[str, Dict[str, Any]] = {}
TASK_LOGS: Dict[str, List[Dict[str, Any]]] = {}
TASK_LOCK = threading.Lock()

RUNNER_THREAD: Optional[threading.Thread] = None
RUNNER_STOP = threading.Event()

PENDING_QUEUE: List[str] = []
QUEUE_COND = threading.Condition(TASK_LOCK)

CURRENT_TASK_ID: Optional[str] = None

PERCENT_RE = re.compile(r"(\d{1,3})%")

PRODUCT_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".ts"}

def _safe_unlink(p: Path) -> bool:
    try:
        if p.exists():
            p.unlink()
        return True
    except Exception:
        return False

def _reset_webui_default_state(clear_download_db: bool = True):
    """恢复 WebUI 默认状态

    每次“非批量下载”前恢复默认状态：
    - 历史记录不带上一次下载
    - /db/downloaded.db 不带上一次下载
    """
    global CURRENT_TASK_ID, RUNNER_THREAD

    RUNNER_STOP.clear()
    _force_work_flag("0")

    with QUEUE_COND:
        PENDING_QUEUE.clear()

    with TASK_LOCK:
        TASKS.clear()
        TASK_LOGS.clear()
        CURRENT_TASK_ID = None

    _safe_unlink(APP_STATE_PATH)
    _update_app_state({"current_task_id": None, "tasks": {}, "plan": [], "plan_updated_at": _now_ts()})

    if clear_download_db:
        try:
            DOWNLOAD_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        _safe_unlink(DOWNLOAD_DB_PATH)

    try:
        if RUNNER_THREAD is not None and (not RUNNER_THREAD.is_alive()):
            RUNNER_THREAD = None
    except Exception:
        pass

def _now_ts() -> float:
    return time.time()

def _append_log(task_id: str, line: str):
    with TASK_LOCK:
        TASK_LOGS.setdefault(task_id, []).append({"ts": _now_ts(), "line": line})
        if len(TASK_LOGS[task_id]) > 5000:
            TASK_LOGS[task_id] = TASK_LOGS[task_id][-5000:]

def _update_app_state(patch: Dict[str, Any]):
    """合并更新 /tmp/nassav_webui_state.json，避免覆盖其他字段（如 tasks/plan）"""
    try:
        APP_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    try:
        old: Dict[str, Any] = {}
        try:
            if APP_STATE_PATH.exists():
                old = json.loads(APP_STATE_PATH.read_text(encoding="utf-8")) or {}
        except Exception:
            old = {}

        if not isinstance(old, dict):
            old = {}

        old.update(patch)
        APP_STATE_PATH.write_text(json.dumps(old, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _persist_state():
    try:
        state = {
            "current_task_id": CURRENT_TASK_ID,
            "tasks": TASKS,
        }
        _update_app_state(state)
    except Exception:
        pass

def _load_state():
    global CURRENT_TASK_ID
    try:
        if APP_STATE_PATH.exists():
            state = json.loads(APP_STATE_PATH.read_text(encoding="utf-8"))
            CURRENT_TASK_ID = state.get("current_task_id")
            tasks = state.get("tasks") or {}
            if isinstance(tasks, dict):
                TASKS.update(tasks)
    except Exception:
        pass

def _persist_task(task_id: str):
    _persist_state()

def _detect_save_path() -> str:
    cfg_path = BASE_DIR / "cfg" / "configs.json"
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        sp = cfg.get("SavePath") or "./MissAV"
        if sp.startswith("./"):
            sp = str((BASE_DIR / sp[2:]).resolve())
        elif not sp.startswith("/"):
            sp = str((BASE_DIR / sp).resolve())
        return sp
    except Exception:
        return str((BASE_DIR / "MissAV").resolve())

def _force_work_flag(val: str):
    try:
        work_file = BASE_DIR / "work"
        work_file.write_text(val, encoding="utf-8")
    except Exception:
        pass

def _safe_remove_plate_dir(save_path: str, plate: str) -> bool:
    """删除 /NASSAV/MissAV/<PLATE> 目录"""
    try:
        base = Path(save_path).resolve()
        target = (base / plate).resolve()
        if base in target.parents and target.exists() and target.is_dir():
            shutil.rmtree(str(target), ignore_errors=True)
        return True
    except Exception:
        return False

def _guess_product_file(target_dir: Path, plate: str) -> Optional[Path]:
    """寻找下载完成后的成品文件"""
    if not target_dir.exists():
        return None
    for ext in PRODUCT_EXTS:
        p = target_dir / f"{plate}{ext}"
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            return p
    candidates = []
    for f in target_dir.iterdir():
        if f.is_file() and f.suffix.lower() in PRODUCT_EXTS and f.stat().st_size > 0:
            candidates.append(f)
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.stat().st_size, reverse=True)
    return candidates[0]

def _maybe_cleanup_download_db(save_path: str, plate: str) -> bool:
    """允许删除目录后重新下载

    先查 /db/downloaded.db 判断是否下载过
    若已删除成品目录，则清理 downloaded.db，避免“已下载过”导致直接失败
    """
    try:
        if not DOWNLOAD_DB_PATH.exists():
            return False

        target_dir = Path(save_path) / plate
        product = _guess_product_file(target_dir, plate)
        if (not target_dir.exists()) or (product is None):
            _safe_unlink(DOWNLOAD_DB_PATH)
            return True
    except Exception:
        pass
    return False

def _iter_sse_json(obj: Dict[str, Any]):
    yield f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

def _task_init(task_id: str, plate: str):
    with TASK_LOCK:
        TASKS[task_id] = {
            "task_id": task_id,
            "plate": plate,
            "status": "queued",
            "percent": 0,
            "message": "",
            "created_at": _now_ts(),
            "updated_at": _now_ts(),
        }
        TASK_LOGS[task_id] = []
    _persist_task(task_id)

def _set_current_task(task_id: Optional[str]):
    global CURRENT_TASK_ID
    CURRENT_TASK_ID = task_id
    _persist_state()

def run_download(task_id: str, plate: str):
    save_path = _detect_save_path()
    target_dir = Path(save_path) / plate

    cleaned_db = _maybe_cleanup_download_db(save_path, plate)

    _append_log(task_id, f"[INFO] SavePath：{save_path}")
    _append_log(task_id, f"[INFO] 目标目录：{target_dir}")
    _append_log(task_id, f"[INFO] 执行：{BASE_DIR}/bin/python main.py {plate}")
    if cleaned_db:
        _append_log(task_id, f"[INFO] 检测到目录缺失/无成品，已清理：{DOWNLOAD_DB_PATH}")

    last_percent = 0
    saw_running_singleton_msg = False

    try:
        cmd = [str(BASE_DIR / "bin" / "python"), "main.py", plate]
        proc = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        with TASK_LOCK:
            TASKS[task_id]["pid"] = proc.pid
            TASKS[task_id]["updated_at"] = _now_ts()
        _persist_task(task_id)

        assert proc.stdout is not None
        for raw in proc.stdout:
            if RUNNER_STOP.is_set():
                break

            line = raw.rstrip("\n")
            if not line:
                continue

            _append_log(task_id, line)

            m = PERCENT_RE.search(line)
            if m:
                try:
                    p = int(m.group(1))
                    p = max(0, min(100, p))
                    if p >= 1:
                        last_percent = p
                    with TASK_LOCK:
                        TASKS[task_id].update(
                            {"percent": p, "updated_at": _now_ts(), "status": "running"}
                        )
                    _persist_task(task_id)
                except Exception:
                    pass
            else:
                pass

            if "A download task is running" in line or "download queue" in line:
                saw_running_singleton_msg = True

        if RUNNER_STOP.is_set():
            try:
                proc.terminate()
            except Exception:
                pass

            _force_work_flag("0")
            removed = _safe_remove_plate_dir(save_path, plate)
            _append_log(task_id, f"[INFO] 已停止，删除目录：{target_dir} -> {'OK' if removed else 'FAIL'}")

            with TASK_LOCK:
                TASKS[task_id].update(
                    {"status": "stopped", "percent": last_percent, "message": "已停止", "updated_at": _now_ts()}
                )
            _persist_task(task_id)
            return

        code = proc.wait(timeout=10)

        if saw_running_singleton_msg:
            _append_log(task_id, "[ERROR] main.py 仍检测到 work=1（单例锁异常），已强制写回 0，请重试。")
            _force_work_flag("0")
            removed = _safe_remove_plate_dir(save_path, plate)
            _append_log(task_id, f"[INFO] 删除目录：{target_dir} -> {'OK' if removed else 'FAIL'}")
            with TASK_LOCK:
                TASKS[task_id].update(
                    {"status": "error", "percent": last_percent, "message": "失败（main.py 单例锁 work=1）", "updated_at": _now_ts()}
                )
            _persist_task(task_id)
            return

        product = _guess_product_file(target_dir, plate)

        if code == 0 and product:
            with TASK_LOCK:
                TASKS[task_id].update(
                    {"status": "done", "percent": 100, "message": "完成", "updated_at": _now_ts(), "product": str(product)}
                )
            _append_log(task_id, f"[INFO] 完成：{product.name}")
            _persist_task(task_id)
            return
        else:
            msg = f"失败（exit_code={code}，未识别到成品）"
            _append_log(task_id, f"[ERROR] {msg}")
            removed = _safe_remove_plate_dir(save_path, plate)
            _append_log(task_id, f"[INFO] 删除目录：{target_dir} -> {'OK' if removed else 'FAIL'}")
            with TASK_LOCK:
                TASKS[task_id].update(
                    {"status": "error", "percent": last_percent, "message": msg, "updated_at": _now_ts()}
                )
            _persist_task(task_id)
            return

    except Exception as e:
        with TASK_LOCK:
            TASKS[task_id].update({"status": "error", "message": f"运行异常：{e}", "updated_at": _now_ts()})
        _append_log(task_id, f"[ERROR] 运行异常：{e}")
        try:
            removed = _safe_remove_plate_dir(save_path, plate)
            _append_log(task_id, f"[INFO] 删除目录：{target_dir} -> {'OK' if removed else 'FAIL'}")
        except Exception:
            pass
        _persist_task(task_id)

def runner_loop():
    """按入队顺序逐个执行"""
    global RUNNER_THREAD
    try:
        while not RUNNER_STOP.is_set():
            with QUEUE_COND:
                if not PENDING_QUEUE:
                    break
                task_id = PENDING_QUEUE.pop(0)

            with TASK_LOCK:
                t = TASKS.get(task_id)
                plate = (t.get("plate") if t else "") or ""
                plate = plate.strip().upper()
                if not t or not plate:
                    continue
                t.update({"status": "running", "updated_at": _now_ts()})
            _persist_task(task_id)

            _set_current_task(task_id)
            run_download(task_id, plate)

        _set_current_task(None)
    finally:
        RUNNER_STOP.clear()
        RUNNER_THREAD = None
        _persist_state()

@app.get("/", response_class=HTMLResponse)
def index():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")

@app.post("/api/plan")
def api_plan(plates: str = Form(...)):
    raw = plates or ""
    lines = [x.strip().upper() for x in re.split(r"\r?\n", raw) if x.strip()]
    seen = set()
    out = []
    for p in lines:
        if p not in seen:
            seen.add(p)
            out.append(p)

    with TASK_LOCK:
        _update_app_state({"plan": out, "plan_updated_at": _now_ts()})

    return JSONResponse({"ok": True, "count": len(out)})

@app.post("/api/start")
def api_start(plate: str = Form(...)):
    global RUNNER_THREAD
    raw = plate or ""
    lines = [x.strip().upper() for x in re.split(r"\r?\n", raw) if x.strip()]
    seen = set()
    plates: List[str] = []
    for p in lines:
        if p not in seen:
            seen.add(p)
            plates.append(p)

    if not plates:
        return JSONResponse({"error": "empty"}, status_code=400)

    is_batch = len(plates) > 1

    try:
        busy = False
        with TASK_LOCK:
            if CURRENT_TASK_ID and CURRENT_TASK_ID in TASKS:
                st = (TASKS[CURRENT_TASK_ID].get("status") or "").lower()
                if st in ("running", "queued"):
                    busy = True
        with QUEUE_COND:
            if PENDING_QUEUE:
                busy = True
        if RUNNER_THREAD is not None and RUNNER_THREAD.is_alive():
            busy = True
        if busy:
            return JSONResponse({"error": "busy"}, status_code=409)
    except Exception:
        pass

    if not is_batch:
        _reset_webui_default_state(clear_download_db=True)
        _update_app_state({"plan": [], "plan_updated_at": _now_ts()})
    else:
        _update_app_state({"plan": plates, "plan_updated_at": _now_ts()})

    task_ids: List[str] = []
    first_task_id: Optional[str] = None
    for p in plates:
        tid = uuid.uuid4().hex
        _task_init(tid, p)
        task_ids.append(tid)
        if first_task_id is None:
            first_task_id = tid

    with QUEUE_COND:
        PENDING_QUEUE.extend(task_ids)

    if RUNNER_THREAD is None or not RUNNER_THREAD.is_alive():
        RUNNER_STOP.clear()
        RUNNER_THREAD = threading.Thread(target=runner_loop, daemon=True)
        RUNNER_THREAD.start()

    return JSONResponse({"task_id": first_task_id, "count": len(plates), "batch": is_batch})

@app.post("/api/stop")
def api_stop():
    """
    停止当前下载，并清空“等待中”的列表：
    - 清空内存队列 PENDING_QUEUE
    - 清空 state.json 中的 plan
    - 将尚未开始的 queued 任务标记为 stopped
    """
    RUNNER_STOP.set()

    _force_work_flag("0")

    try:
        with QUEUE_COND:
            pending_ids = list(PENDING_QUEUE)
            PENDING_QUEUE.clear()

        with TASK_LOCK:
            for tid in pending_ids:
                t = TASKS.get(tid)
                if t and t.get("status") == "queued":
                    t.update(
                        {
                            "status": "stopped",
                            "message": "已停止",
                            "updated_at": _now_ts(),
                        }
                    )
        _persist_state()
    except Exception:
        pass

    try:
        if CURRENT_TASK_ID and CURRENT_TASK_ID in TASKS:
            pid = TASKS[CURRENT_TASK_ID].get("pid")
            if pid:
                try:
                    os.kill(int(pid), 15)
                except Exception:
                    pass
    except Exception:
        pass

    with TASK_LOCK:
        _update_app_state({"plan": [], "plan_updated_at": _now_ts()})

    return JSONResponse({"ok": True})

@app.get("/api/status")
def api_status():
    """恢复当前正在运行的任务状态"""
    with TASK_LOCK:
        tid = CURRENT_TASK_ID
        if not tid or tid not in TASKS:
            return JSONResponse({"running": False})
        t = TASKS[tid].copy()
    return JSONResponse({"running": True, "task_id": tid, **t})

@app.get("/api/history")
def api_history():
    """提供历史：done/waiting/error"""
    with TASK_LOCK:
        plan = []
        try:
            if APP_STATE_PATH.exists():
                old = json.loads(APP_STATE_PATH.read_text(encoding="utf-8"))
                plan = old.get("plan") or []
        except Exception:
            plan = []

        done = []
        error = []
        running_plate = None

        for tid, t in TASKS.items():
            st = t.get("status")
            p = (t.get("plate") or "").upper()
            if st == "done":
                done.append(p)
            elif st == "error":
                error.append(p)
            elif st == "running":
                running_plate = p

        done_set = set(done)
        err_set = set(error)
        waiting = []
        for p in [x.upper() for x in plan]:
            if p and (p not in done_set) and (p not in err_set) and (p != running_plate):
                waiting.append(p)

    return JSONResponse({"done": done, "waiting": waiting, "error": error})

@app.get("/api/progress/{task_id}")
def api_progress(task_id: str):
    with TASK_LOCK:
        t = TASKS.get(task_id)
        if not t:
            return JSONResponse({"error": "notfound"}, status_code=404)
        return JSONResponse(t)

@app.get("/api/progress/{task_id}/stream")
def api_progress_stream(task_id: str):
    def gen():
        while True:
            with TASK_LOCK:
                t = TASKS.get(task_id)
            if not t:
                yield from _iter_sse_json({"task_id": task_id, "status": "error", "percent": 0})
                return
            yield from _iter_sse_json(t)
            if t.get("status") in ("done", "error", "stopped"):
                return
            time.sleep(0.5)

    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/logs/{task_id}/stream")
def api_logs_stream(task_id: str):
    def gen():
        idx = 0
        while True:
            with TASK_LOCK:
                logs = TASK_LOGS.get(task_id, [])
                t = TASKS.get(task_id)
            if idx < len(logs):
                for j in range(idx, len(logs)):
                    yield f"data: {json.dumps(logs[j], ensure_ascii=False)}\n\n"
                idx = len(logs)

            if t and t.get("status") in ("done", "error", "stopped"):
                with TASK_LOCK:
                    logs2 = TASK_LOGS.get(task_id, [])
                if idx < len(logs2):
                    for j in range(idx, len(logs2)):
                        yield f"data: {json.dumps(logs2[j], ensure_ascii=False)}\n\n"
                return

            time.sleep(0.35)

    return StreamingResponse(gen(), media_type="text/event-stream")

_load_state()
