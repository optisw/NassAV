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

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

APP_ROOT = os.environ.get("NASSAV_ROOT", "/NASSAV")
CONFIG_PATH = os.environ.get("NASSAV_CONFIG", os.path.join(APP_ROOT, "cfg", "configs.json"))
VENV_PY = os.path.join(APP_ROOT, "bin", "python")

# ---- 读取配置（主要为了拿 SavePath，找到最终 mp4）----
def load_cfg() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError(f"无法读取配置文件：{CONFIG_PATH}。错误：{e}")

def get_save_path(cfg: dict) -> str:
    sp = cfg.get("SavePath")
    if not sp:
        raise RuntimeError("configs.json 缺少 SavePath 字段")
    if not os.path.isabs(sp):
        sp = os.path.normpath(os.path.join(APP_ROOT, sp))
    return sp

# ---- 任务状态存储（内存）----
# status: queued | running | done | error | stopped
TASKS: Dict[str, dict] = {}
TASK_QUEUE: "queue.Queue[dict]" = queue.Queue()

PERCENT_RE = re.compile(r"(\d{1,3})\s*%")
MAX_LOG_LINES = 3000  # 每个任务最多保留日志行数

# ---- 停止/取消控制 ----
CONTROL_LOCK = threading.Lock()
CURRENT_TASK_ID: Optional[str] = None
CURRENT_PROC: Optional[subprocess.Popen] = None
CANCEL_ALL_FLAG = False  # 一键停止标志

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
        drop = len(logs) - MAX_LOG_LINES
        del logs[:drop]

def _safe_remove_plate_dir(save_path: str, plate: str) -> bool:
    """
    删除 SavePath/<PLATE>/ 目录（仅允许在 SavePath 内删除，避免误删）
    """
    plate = normalize_plate(plate)
    target = os.path.normpath(os.path.join(save_path, plate))
    save_path_norm = os.path.normpath(save_path)

    # 安全校验：target 必须在 save_path 下
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

def _is_cancel_requested() -> bool:
    with CONTROL_LOCK:
        return CANCEL_ALL_FLAG

def run_download(task_id: str, plate: str):
    global CURRENT_TASK_ID, CURRENT_PROC

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

    _append_log(task_id, f"[INFO] 使用解释器：{VENV_PY}")
    _append_log(task_id, f"[INFO] 工作目录：{APP_ROOT}")
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

    # 记录当前运行的进程（供 stop 使用）
    with CONTROL_LOCK:
        CURRENT_TASK_ID = task_id
        CURRENT_PROC = proc

    last_percent = 0
    try:
        assert proc.stdout is not None
        for raw in proc.stdout:
            if _is_cancel_requested():
                _append_log(task_id, "[INFO] 收到停止指令，正在终止…")
                break

            line = raw.rstrip("\n")
            if not line:
                continue

            _append_log(task_id, line)

            m = PERCENT_RE.search(line)
            if m:
                p = int(m.group(1))
                p = max(0, min(100, p))
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

        # 如果是取消：杀进程并删目录
        if _is_cancel_requested():
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass

            _safe_remove_plate_dir(save_path, plate)

            TASKS[task_id].update({
                "status": "stopped",
                "percent": last_percent,
                "message": "已停止",
                "updated_at": time.time(),
            })
            _append_log(task_id, "[INFO] 已停止任务并清理目录")
            return

        code = proc.wait()
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
            if out:
                _append_log(task_id, f"[INFO] 成品文件：{out}")
            else:
                _append_log(task_id, "[WARN] 未找到 mp4（可能输出命名不同或仍在转换）")
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
        # 清理当前进程引用
        with CONTROL_LOCK:
            if CURRENT_TASK_ID == task_id:
                CURRENT_TASK_ID = None
                CURRENT_PROC = None

def worker_loop():
    global CANCEL_ALL_FLAG
    while True:
        item = TASK_QUEUE.get()
        try:
            # 如果停止标志已经置位，直接把队列任务标记为 stopped（不执行）
            if _is_cancel_requested():
                tid = item["task_id"]
                TASKS[tid].update({
                    "status": "stopped",
                    "percent": 0,
                    "message": "已停止（未开始执行）",
                    "updated_at": time.time(),
                })
                _append_log(tid, "[INFO] 队列任务已停止（未开始执行）")
                continue

            task_id = item["task_id"]
            plate = item["plate"]
            run_download(task_id, plate)
        finally:
            TASK_QUEUE.task_done()

app = FastAPI(title="AVTool WebUI")

STATIC_DIR = os.path.join(APP_ROOT, "webui", "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

t = threading.Thread(target=worker_loop, daemon=True)
t.start()

@app.get("/", response_class=HTMLResponse)
def index():
    html_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h3>index.html not found</h3>", status_code=500)
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
    - 终止正在运行的下载进程
    - 清空队列中的后续任务
    - 删除正在下载车牌对应目录
    """
    global CANCEL_ALL_FLAG, CURRENT_PROC, CURRENT_TASK_ID

    with CONTROL_LOCK:
        CANCEL_ALL_FLAG = True
        proc = CURRENT_PROC
        running_tid = CURRENT_TASK_ID

    # 先杀进程（run_download 会负责删目录和标记 stopped）
    if proc is not None:
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass

    # 清空队列（把未执行的任务标为 stopped）
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

    # 停止只对当前队列有效，恢复开关，便于下一次重新下载
    with CONTROL_LOCK:
        CANCEL_ALL_FLAG = False

    return {"ok": True, "running_task_id": running_tid, "cleared": cleared}

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
