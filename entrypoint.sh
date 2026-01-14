#!/usr/bin/env bash
set -e

cd /NASSAV

# 启动 WebUI（容器会一直保持运行）
exec uvicorn webui.app:app --host 0.0.0.0 --port 8008
