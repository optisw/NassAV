import os
import re
import json
import time
import uuid
import queue
import threading
import subprocess
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
    # 相对路径统一转成容器内绝对路径（以 /NASSAV 为基准）
    if not os.path.isabs(sp):
        sp = os.path.normpath(os.path.join(APP_ROOT, sp))
    return sp

# ---- 任务状态存储（内存）----
# status: queued | running | done | error
TASKS: Dict[str, dict] = {}
TASK_QUEUE: "queue.Queue[dict]" = queue.Queue()

PERCENT_RE = re.compile(r"(\d{1,3})\s*%")

MAX_LOG_LINES = 3000  # 每个任务最多保留日志行数

def normalize_plate(s: str) -> str:
    return s.strip().upper()

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

def _append_log(task_id: str, line: str):
    t = TASKS.get(task_id)
    if not t:
        return
    logs = t.setdefault("logs", [])
    seq = t.setdefault("log_seq", 0) + 1
    t["log_seq"] = seq
    logs.append({"seq": seq, "line": line})
    # 限制行数
    if len(logs) > MAX_LOG_LINES:
        drop = len(logs) - MAX_LOG_LINES
        del logs[:drop]

def run_download(task_id: str, plate: str):
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

    last_percent = 0
    try:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.rstrip("\n")
            if not line:
                continue

            _append_log(task_id, line)

            # 尝试从输出里抓 “xx%”
            m = PERCENT_RE.search(line)
            if m:
                p = int(m.group(1))
                p = max(0, min(100, p))
                if p >= last_percent:
                    last_percent = p
            else:
                # 无百分比时，让 UI 不至于卡死（最多推到 95）
                if last_percent < 95:
                    last_percent = min(95, last_percent + 1)

            TASKS[task_id].update({
                "percent": last_percent,
                "message": line[-200:],
                "updated_at": time.time(),
            })

        code = proc.wait()
        if code == 0:
            out = guess_output_file(save_path, plate)
            TASKS[task_id].update({
                "status": "done",
                "percent": 100,
                "message": "完成，可下载",
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
        try:
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass

def worker_loop():
    while True:
        item = TASK_QUEUE.get()
        try:
            task_id = item["task_id"]
            plate = item["plate"]
            run_download(task_id, plate)
        finally:
            TASK_QUEUE.task_done()

app = FastAPI(title="NASSAV Simple WebUI")

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

@app.post("/api/start_batch")
async def api_start_batch(file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8", errors="ignore")
    plates: List[str] = []
    for line in content.splitlines():
        p = normalize_plate(line)
        if p:
            plates.append(p)

    if not plates:
        raise HTTPException(status_code=400, detail="txt 文件里没有有效车牌号")

    batch_id = uuid.uuid4().hex
    task_ids = []
    for p in plates:
        task_id = uuid.uuid4().hex
        TASKS[task_id] = {
            "task_id": task_id,
            "batch_id": batch_id,
            "plate": p,
            "status": "queued",
            "percent": 0,
            "message": "已进入队列",
            "created_at": time.time(),
            "updated_at": time.time(),
            "logs": [],
            "log_seq": 0,
        }
        task_ids.append(task_id)
        TASK_QUEUE.put({"task_id": task_id, "plate": p})

    return {"batch_id": batch_id, "task_ids": task_ids, "count": len(task_ids)}

@app.get("/api/progress/{task_id}")
def api_progress(task_id: str):
    info = TASKS.get(task_id)
    if not info:
        raise HTTPException(status_code=404, detail="task_id 不存在")
    # 避免一次性把 logs 全返回（太大）
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
            if info.get("status") in ("done", "error"):
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
            # 推送新增行
            new = [x for x in logs if x["seq"] > last_seq]
            for item in new:
                last_seq = item["seq"]
                payload = json.dumps(item, ensure_ascii=False)
                yield f"data: {payload}\n\n"

            if info.get("status") in ("done", "error"):
                # 最后再等一下，确保尾巴输出到位
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
