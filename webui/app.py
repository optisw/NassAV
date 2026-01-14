import os
import re
import json
import time
import uuid
import queue
import threading
import subprocess
import shutil
import signal
import select
from typing import Dict, Optional, List

from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

APP_ROOT = os.environ.get("NASSAV_ROOT", "/NASSAV")
CONFIG_PATH = os.environ.get("NASSAV_CONFIG", os.path.join(APP_ROOT, "cfg", "configs.json"))
VENV_PY = os.path.join(APP_ROOT, "bin", "python")

WORK_FLAG_PATH = os.path.join(APP_ROOT, "work")
DB_DIR = os.path.join(APP_ROOT, "db")
QUEUE_FILE_PATH = os.path.join(DB_DIR, "download_queue.txt")

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts", ".m4v"}

# 1) 解析 "173/1731" 这种真实进度（优先）
RATIO_RE = re.compile(r"\b(\d+)\s*/\s*(\d+)\b")
# 2) 解析 "9%" 这种百分比（备用）
PERCENT_RE = re.compile(r"\b(100|[1-9]?\d)\s*%\b")

MAX_LOG_LINES = 3000

TASKS: Dict[str, dict] = {}
TASK_QUEUE: "queue.Queue[dict]" = queue.Queue()

CONTROL_LOCK = threading.Lock()
STOP_TOKEN = 0

CURRENT_TASK_ID: Optional[str] = None
CURRENT_PROC: Optional[subprocess.Popen] = None
CURRENT_PGID: Optional[int] = None
CURRENT_PLATE: Optional[str] = None
CURRENT_SAVE_PATH: Optional[str] = None


# ---------------- Config ----------------

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


# ---------------- Utils ----------------

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

def _current_stop_token() -> int:
    with CONTROL_LOCK:
        return STOP_TOKEN

def _target_dir(save_path: str, plate: str) -> str:
    save_path_norm = os.path.normpath(save_path)
    return os.path.normpath(os.path.join(save_path_norm, normalize_plate(plate)))

def _safe_remove_plate_dir(save_path: str, plate: str, retries: int = 12, delay: float = 0.2) -> bool:
    save_path_norm = os.path.normpath(save_path)
    target = _target_dir(save_path, plate)
    if not (target == save_path_norm or target.startswith(save_path_norm + os.sep)):
        return False
    for _ in range(retries):
        if os.path.isdir(target):
            try:
                shutil.rmtree(target, ignore_errors=True)
            except Exception:
                pass
        if not os.path.exists(target):
            return True
        time.sleep(delay)
    return not os.path.exists(target)

def _killpg_soft_then_hard(pgid: int):
    try:
        os.killpg(pgid, signal.SIGTERM)
    except Exception:
        pass
    time.sleep(0.25)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except Exception:
        pass

def _wait_exit(proc: subprocess.Popen, timeout: float = 2.0):
    try:
        proc.wait(timeout=timeout)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=1.0)
        except Exception:
            pass

def _list_video_files(folder: str) -> List[str]:
    out = []
    if not os.path.isdir(folder):
        return out
    for fn in os.listdir(folder):
        fp = os.path.join(folder, fn)
        if not os.path.isfile(fp):
            continue
        if os.path.splitext(fn)[1].lower() in VIDEO_EXTS:
            out.append(fp)
    return out

def pick_product_file(save_path: str, plate: str) -> Optional[str]:
    folder = os.path.join(save_path, normalize_plate(plate))
    files = _list_video_files(folder)
    if not files:
        return None
    def sz(p):
        try:
            return os.path.getsize(p)
        except Exception:
            return 0
    files.sort(key=sz, reverse=True)
    return files[0]

def wait_for_product(save_path: str, plate: str, seconds: float = 25.0) -> Optional[str]:
    deadline = time.time() + seconds
    while time.time() < deadline:
        p = pick_product_file(save_path, plate)
        if p:
            return p
        time.sleep(0.5)
    return pick_product_file(save_path, plate)


# ---------------- Singleton fix (work) ----------------

def _force_work_flag(value: str = "0"):
    try:
        with open(WORK_FLAG_PATH, "w", encoding="utf-8") as f:
            f.write(value)
    except Exception:
        pass

def _clear_internal_queue_file():
    try:
        os.makedirs(DB_DIR, exist_ok=True)
        with open(QUEUE_FILE_PATH, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass

def _preflight_fix_singleton():
    _force_work_flag("0")
    _clear_internal_queue_file()


# ---------------- Stream read (supports \r) ----------------

def _iter_console_lines(proc: subprocess.Popen):
    """
    bytes 流读取 stdout，按 \n 或 \r 切分，捕获 m3u8-Downloader-Go 的进度刷新
    """
    assert proc.stdout is not None
    fd = proc.stdout.fileno()
    buf = b""
    while True:
        if proc.poll() is not None:
            if buf:
                s = buf.decode(errors="ignore").strip()
                if s:
                    yield s
            break

        r, _, _ = select.select([fd], [], [], 0.2)
        if not r:
            continue

        try:
            chunk = os.read(fd, 4096)
        except Exception:
            break
        if not chunk:
            continue

        buf += chunk
        while True:
            npos = buf.find(b"\n")
            rpos = buf.find(b"\r")
            if npos == -1 and rpos == -1:
                break
            if npos == -1:
                cut = rpos
            elif rpos == -1:
                cut = npos
            else:
                cut = min(npos, rpos)

            line = buf[:cut]
            buf = buf[cut + 1:]
            s = line.decode(errors="ignore").strip()
            if s:
                yield s


def _progress_from_line(line: str) -> Optional[int]:
    """
    优先：done/total
    备用：xx%
    """
    m = RATIO_RE.search(line)
    if m:
        try:
            done = int(m.group(1))
            total = int(m.group(2))
            if total > 0:
                p = int(done * 100 / total)
                return max(0, min(100, p))
        except Exception:
            pass

    m = PERCENT_RE.search(line)
    if m:
        try:
            return max(0, min(100, int(m.group(1))))
        except Exception:
            return None
    return None


# ---------------- Core download ----------------

def run_download(task_id: str, plate: str):
    global CURRENT_TASK_ID, CURRENT_PROC, CURRENT_PGID, CURRENT_PLATE, CURRENT_SAVE_PATH

    my_token = _current_stop_token()

    try:
        cfg = load_cfg()
        save_path = get_save_path(cfg)
    except Exception as e:
        TASKS[task_id].update({"status": "error", "percent": 0, "message": f"配置读取失败：{e}", "updated_at": time.time()})
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

    target_dir = _target_dir(save_path, plate)
    _append_log(task_id, f"[INFO] SavePath：{save_path}")
    _append_log(task_id, f"[INFO] 目标目录：{target_dir}")
    _append_log(task_id, f"[INFO] 执行：{VENV_PY} main.py {plate}")

    _preflight_fix_singleton()

    cmd = [VENV_PY, "main.py", plate]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
        cmd,
        cwd=APP_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=0,
        env=env,
        preexec_fn=os.setsid,
    )
    pgid = proc.pid

    with CONTROL_LOCK:
        CURRENT_TASK_ID = task_id
        CURRENT_PROC = proc
        CURRENT_PGID = pgid
        CURRENT_PLATE = plate
        CURRENT_SAVE_PATH = save_path

    last_percent = 0
    stopped = False
    saw_running_singleton_msg = False

    try:
        for line in _iter_console_lines(proc):
            if _current_stop_token() != my_token:
                stopped = True
                _append_log(task_id, "[INFO] 收到停止指令，终止并清理…")
                _killpg_soft_then_hard(pgid)
                _force_work_flag("0")
                break

            if "A download task is running, save" in line:
                saw_running_singleton_msg = True

            p = _progress_from_line(line)
            if p is not None:
                # 进程没结束前，最多 99，避免“合并收尾中”也显示 100
                if p >= 100 and proc.poll() is None:
                    p = 99
                if p > last_percent:
                    last_percent = p
                    TASKS[task_id].update({"percent": last_percent, "updated_at": time.time()})
                # 进度行也写进日志（你现在需要可视化）
                _append_log(task_id, line)
                continue

            _append_log(task_id, line)

        _wait_exit(proc, timeout=2.0)
        code = proc.poll()

        if stopped or (_current_stop_token() != my_token):
            removed = _safe_remove_plate_dir(save_path, plate)
            _append_log(task_id, f"[INFO] 已停止：exit_code={code}")
            _append_log(task_id, f"[INFO] 删除目录：{target_dir} -> {'OK' if removed else 'FAIL'}")
            TASKS[task_id].update({"status": "stopped", "percent": 0, "message": "已停止", "updated_at": time.time()})
            return

        if saw_running_singleton_msg:
            _append_log(task_id, "[ERROR] main.py 仍检测到 work=1（单例锁异常），已强制写回 0，请重试。")
            _force_work_flag("0")
            TASKS[task_id].update({"status": "error", "percent": last_percent, "message": "失败（main.py 单例锁 work=1）", "updated_at": time.time()})
            return

        product = None
        if code == 0:
            product = wait_for_product(save_path, plate, seconds=25.0)
        else:
            product = pick_product_file(save_path, plate)

        if code == 0 and product:
            _append_log(task_id, f"[INFO] 识别到成品：{product}")
            TASKS[task_id].update({"status": "done", "percent": 100, "message": "完成", "file": product, "updated_at": time.time()})
        else:
            msg = f"失败（exit_code={code}，未识别到成品）"
            _append_log(task_id, f"[ERROR] {msg}")
            TASKS[task_id].update({"status": "error", "percent": last_percent, "message": msg, "updated_at": time.time()})

    except Exception as e:
        TASKS[task_id].update({"status": "error", "message": f"运行异常：{e}", "updated_at": time.time()})
        _append_log(task_id, f"[ERROR] 运行异常：{e}")

    finally:
        with CONTROL_LOCK:
            if CURRENT_TASK_ID == task_id:
                CURRENT_TASK_ID = None
                CURRENT_PROC = None
                CURRENT_PGID = None
                CURRENT_PLATE = None
                CURRENT_SAVE_PATH = None


def worker_loop():
    while True:
        item = TASK_QUEUE.get()
        try:
            run_download(item["task_id"], item["plate"])
        finally:
            TASK_QUEUE.task_done()


# ---------------- FastAPI ----------------

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
    global STOP_TOKEN

    with CONTROL_LOCK:
        STOP_TOKEN += 1
        token_now = STOP_TOKEN
        task_id = CURRENT_TASK_ID
        plate = CURRENT_PLATE
        save_path = CURRENT_SAVE_PATH
        pgid = CURRENT_PGID

    if pgid is not None:
        _killpg_soft_then_hard(pgid)

    _force_work_flag("0")
    _clear_internal_queue_file()

    removed = False
    running_dir = None
    if save_path and plate:
        running_dir = _target_dir(save_path, plate)
        removed = _safe_remove_plate_dir(save_path, plate)

    # 清空 WebUI 队列
    cleared = 0
    while True:
        try:
            item = TASK_QUEUE.get_nowait()
        except queue.Empty:
            break
        try:
            tid = item["task_id"]
            TASKS[tid].update({"status": "stopped", "percent": 0, "message": "已停止（从队列移除）", "updated_at": time.time()})
            _append_log(tid, "[INFO] 已停止（从队列移除）")
            cleared += 1
        finally:
            TASK_QUEUE.task_done()

    if task_id and task_id in TASKS:
        TASKS[task_id].update({"status": "stopped", "percent": 0, "message": "已停止", "updated_at": time.time()})
        _append_log(task_id, f"[INFO] Stop：work=0 已写入，队列已清空，removed_dir={'OK' if removed else 'FAIL'}")

    return {
        "ok": True,
        "stop_token": token_now,
        "running_task_id": task_id,
        "running_plate": plate,
        "cleared": cleared,
        "removed_running_dir": removed,
        "running_dir": running_dir
    }


@app.get("/api/progress/{task_id}/stream")
def api_progress_stream(task_id: str):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="task_id 不存在")

    # ✅ 固定频率推送，不依赖“payload变化才推送”
    def gen():
        while True:
            info = TASKS.get(task_id)
            if not info:
                break
            slim = dict(info)
            slim.pop("logs", None)
            yield f"data: {json.dumps(slim, ensure_ascii=False)}\n\n"
            if info.get("status") in ("done", "error", "stopped"):
                break
            time.sleep(0.2)

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
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
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

    product = pick_product_file(save_path, plate)
    if not product or not os.path.exists(product):
        raise HTTPException(status_code=404, detail="未找到可下载成品（可能还在下载/转换）")

    return FileResponse(product, filename=os.path.basename(product), media_type="application/octet-stream")
