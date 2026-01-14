import os
import re
import json
import time
import uuid
import queue
import threading
import subprocess
import shutil
from typing import Dict, Optional, List

from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

APP_ROOT = os.environ.get("NASSAV_ROOT", "/NASSAV")
CONFIG_PATH = os.environ.get("NASSAV_CONFIG", os.path.join(APP_ROOT, "cfg", "configs.json"))
VENV_PY = os.path.join(APP_ROOT, "bin", "python")

def load_cfg() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_save_path(cfg: dict) -> str:
    sp = cfg.get("SavePath")
    if not sp:
        raise RuntimeError("configs.json 缺少 SavePath 字段")
    if not os.path.isabs(sp):
        sp = os.path.normpath(os.path.join(APP_ROOT, sp))
    return sp

TASKS: Dict[str, dict] = {}
TASK_QUEUE: "queue.Queue[dict]" = queue.Queue()

PERCENT_RE = re.compile(r"(\d{1,3})\s*%")
MAX_LOG_LINES = 3000

# ---- 停止控制：用 Event 避免竞态 ----
STOP_EVENT = threading.Event()

CONTROL_LOCK = threading.Lock()
CURRENT_TASK_ID: Optional[str] = None
CURRENT_PROC: Optional[subprocess.Popen] = None
CURRENT_PLATE: Optional[str] = None
CURRENT_SAVE_PATH: Optional[str] = None

def normalize_plate(s: str) -> str:
    return s.strip().upper()

def _append_log(task_id: str, line: str):
    t = TASKS.get(task_id)
    if not t:
        return
    logs = t.setdefault("logs", [])
    seq = t.setdefault("log_seq", 0) + 1
    t["log_seq"] = seq
    logs.append({"seq": seq, "line": line})
    if len(logs) > MAX_LOG_LINES:
        del logs[: len(logs) - MAX_LOG_LINES]

def _safe_remove_plate_dir(save_path: str, plate: str) -> bool:
    """
    删除 SavePath/<PLATE>/ 目录（并做路径安全校验）
    """
    plate = normalize_plate(plate)
    save_path_norm = os.path.normpath(save_path)
    target = os.path.normpath(os.path.join(save_path_norm, plate))

    # target 必须在 save_path 下
    if not (target == save_path_norm or target.startswith(save_path_norm + os.sep)):
        return False

    if os.path.isdir(target):
        shutil.rmtree(target, ignore_errors=True)
        return True
    return False

def guess_output_file(save_path: str, plate: str) -> Optional[str]:
    plate = normalize_plate(plate)
    cand = os.path.join(save_path, plate, f"{plate}.mp4")
    if os.path.exists(cand):
        return cand
    folder = os.path.join(save_path, plate)
    if os.path.isdir(folder):
        for fn in os.listdir(folder):
            if fn.lower().endswith(".mp4"):
                return os.path.join(folder, fn)
    return None

def run_download(task_id: str, plate: str):
    global CURRENT_TASK_ID, CURRENT_PROC, CURRENT_PLATE, CURRENT_SAVE_PATH

    try:
        cfg = load_cfg()
        save_path = get_save_path(cfg)
    except Exception as e:
        TASKS[task_id].update({
            "status": "error",
            "percent": 0,
            "message": f"配置读取失败：{e}",
            "updated_at": time.time(),
        })
        _append_log(task_id, f"[ERROR] 配置读取失败：{e}")
        return

    TASKS[task_id].update({
        "status": "running",
        "percent": 0,
        "message": "开始下载…",
        "plate": plate,
        "updated_at": time.time(),
        "file": None,
        "logs": [],
        "log_seq": 0,
    })

    _append_log(task_id, f"[INFO] SavePath：{save_path}")
    _append_log(task_id, f"[INFO] 执行：{VENV_PY} main.py {plate}")

    cmd = [VENV_PY, "main.py", plate]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
        cmd,
        cwd=APP_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        env=env,
    )

    with CONTROL_LOCK:
        CURRENT_TASK_ID = task_id
        CURRENT_PROC = proc
        CURRENT_PLATE = plate
        CURRENT_SAVE_PATH = save_path

    last_percent = 0
    try:
        assert proc.stdout is not None

        for raw in proc.stdout:
            if STOP_EVENT.is_set():
                _append_log(task_id, "[INFO] 收到停止指令，正在终止…")
                break

            line = raw.rstrip("\n")
            if not line:
                continue

            _append_log(task_id, line)

            m = PERCENT_RE.search(line)
            if m:
                p = max(0, min(100, int(m.group(1))))
                if p >= last_percent:
                    last_percent = p
            else:
                if last_percent < 95:
                    last_percent = min(95, last_percent + 1)

            TASKS[task_id].update({
                "percent": last_percent,
                "message": line[-200:],
                "updated_at": time.time(),
            })

        # 让进程结束（如果 stop 已经触发，通常这里会很快）
        code = proc.wait()

        # ✅ 无论退出码是什么，只要 STOP_EVENT 触发，就按“停止”处理，并删除目录
        if STOP_EVENT.is_set():
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass

            removed = _safe_remove_plate_dir(save_path, plate)
            _append_log(task_id, f"[INFO] 停止任务：exit_code={code}")
            _append_log(task_id, f"[INFO] 清理目录：{os.path.join(save_path, normalize_plate(plate))} -> {'OK' if removed else 'NOT_FOUND/FAIL'}")

            TASKS[task_id].update({
                "status": "stopped",
                "percent": last_percent,
                "message": "已停止",
                "updated_at": time.time(),
            })
            return

        # 正常结束
        if code == 0:
            out = guess_output_file(save_path, plate)
            TASKS[task_id].update({
                "status": "done",
                "percent": 100,
                "message": "完成",
                "file": out,
                "updated_at": time.time(),
            })
            _append_log(task_id, "[INFO] 任务完成（exit code 0）")
        else:
            TASKS[task_id].update({
                "status": "error",
                "message": f"下载失败，退出码={code}",
                "updated_at": time.time(),
            })
            _append_log(task_id, f"[ERROR] 下载失败，退出码={code}")

    except Exception as e:
        TASKS[task_id].update({
            "status": "error",
            "message": f"运行异常：{e}",
            "updated_at": time.time(),
        })
        _append_log(task_id, f"[ERROR] 运行异常：{e}")

    finally:
        with CONTROL_LOCK:
            CURRENT_TASK_ID = None
            CURRENT_PROC = None
            CURRENT_PLATE = None
            CURRENT_SAVE_PATH = None
        # ✅ 在当前任务收尾后再清 stop 标志，避免竞态
        if STOP_EVENT.is_set():
            STOP_EVENT.clear()

def worker_loop():
    while True:
        item = TASK_QUEUE.get()
        try:
            if STOP_EVENT.is_set():
                tid = item["task_id"]
                TASKS[tid].update({
                    "status": "stopped",
                    "percent": 0,
                    "message": "已停止（未开始执行）",
                    "updated_at": time.time(),
                })
                _append_log(tid, "[INFO] 队列任务已停止（未开始执行）")
                continue

            run_download(item["task_id"], item["plate"])
        finally:
            TASK_QUEUE.task_done()

app = FastAPI(title="AVTool WebUI")

STATIC_DIR = os.path.join(APP_ROOT, "webui", "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

threading.Thread(target=worker_loop, daemon=True).start()

@app.get("/", response_class=HTMLResponse)
def index():
    html_path = os.path.join(STATIC_DIR, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.post("/api/start")
def api_start(plate: str = Form(...)):
    plate = normalize_plate(plate)
    if not plate:
        raise HTTPException(status_code=400, detail="车牌号不能为空")

    task_id = uuid.uuid4().hex
    TASKS[task_id] = {
        "task_id": task_id,
        "plate": plate,
        "status": "queued",
        "percent": 0,
        "message": "已进入队列",
        "created_at": time.time(),
        "updated_at": time.time(),
        "logs": [],
        "log_seq": 0,
    }
    TASK_QUEUE.put({"task_id": task_id, "plate": plate})
    return TASKS[task_id]

@app.post("/api/stop")
def api_stop():
    """
    一键停止：
    - 标记 STOP_EVENT
    - kill 当前进程（如果存在）
    - 清空队列并标记 stopped
    注意：STOP_EVENT 不在这里清，等 run_download 收尾后清，确保能触发删目录。
    """
    STOP_EVENT.set()

    with CONTROL_LOCK:
        proc = CURRENT_PROC
        running_tid = CURRENT_TASK_ID
        running_plate = CURRENT_PLATE
        running_save = CURRENT_SAVE_PATH

    if proc is not None:
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass

    # 清空队列
    cleared = 0
    while True:
        try:
            item = TASK_QUEUE.get_nowait()
        except queue.Empty:
            break
        try:
            tid = item["task_id"]
            TASKS[tid].update({
                "status": "stopped",
                "percent": 0,
                "message": "已停止（从队列移除）",
                "updated_at": time.time(),
            })
            _append_log(tid, "[INFO] 已停止（从队列移除）")
            cleared += 1
        finally:
            TASK_QUEUE.task_done()

    # ✅ 再做一次兜底：如果目录已经创建但 run_download 还未来得及清理，这里也尝试删除
    removed = False
    if running_plate and running_save:
        removed = _safe_remove_plate_dir(running_save, running_plate)

    return {
        "ok": True,
        "running_task_id": running_tid,
        "running_plate": running_plate,
        "cleared": cleared,
        "removed_running_dir": removed,
        "running_dir": (os.path.join(running_save, normalize_plate(running_plate)) if running_plate and running_save else None)
    }

@app.get("/api/progress/{task_id}")
def api_progress(task_id: str):
    info = TASKS.get(task_id)
    if not info:
        raise HTTPException(status_code=404, detail="task_id 不存在")
    slim = dict(info)
    slim.pop("logs", None)
    return slim

@app.get("/api/progress/{task_id}/stream")
def api_progress_stream(task_id: str):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="task_id 不存在")

    def gen():
        last_sent = None
        while True:
            info = TASKS.get(task_id)
            if not info:
                break
            slim = dict(info)
            slim.pop("logs", None)
            payload = json.dumps(slim, ensure_ascii=False)
            if payload != last_sent:
                yield f"data: {payload}\n\n"
                last_sent = payload
            if info.get("status") in ("done", "error", "stopped"):
                break
            time.sleep(0.5)

    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/logs/{task_id}/stream")
def api_logs_stream(task_id: str):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="task_id 不存在")

    def gen():
        last_seq = 0
        while True:
            info = TASKS.get(task_id)
            if not info:
                break
            logs = info.get("logs", [])
            new = [x for x in logs if x["seq"] > last_seq]
            for item in new:
                last_seq = item["seq"]
                payload = json.dumps(item, ensure_ascii=False)
                yield f"data: {payload}\n\n"

            if info.get("status") in ("done", "error", "stopped"):
                time.sleep(0.2)
                break
            time.sleep(0.2)

    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/download/{plate}")
def api_download(plate: str):
    cfg = load_cfg()
    save_path = get_save_path(cfg)
    plate = normalize_plate(plate)

    fp = guess_output_file(save_path, plate)
    if not fp or not os.path.exists(fp):
        raise HTTPException(status_code=404, detail="未找到成品 mp4（可能还在下载/转换）")

    return FileResponse(fp, filename=os.path.basename(fp), media_type="video/mp4")
