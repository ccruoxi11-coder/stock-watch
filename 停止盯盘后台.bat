@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo 运行环境不存在。
  pause
  exit /b 1
)

".venv\Scripts\python.exe" stop_worker.py
pause
