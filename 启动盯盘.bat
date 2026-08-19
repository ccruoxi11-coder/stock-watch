@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [1/3] 正在创建虚拟环境...
  python -m venv .venv
  if errorlevel 1 (
    echo 未找到可用的 Python，请安装 Python 3.10+ 并勾选 Add to PATH。
    pause
    exit /b 1
  )
)

echo [2/3] 正在检查依赖...
".venv\Scripts\python.exe" -m pip install -r requirements.txt -q
if errorlevel 1 (
  echo 依赖安装失败，请检查网络后重试。
  pause
  exit /b 1
)

echo [3/3] 正在启动盯盘工具...
powershell.exe -NoProfile -Command "Start-Process -FilePath '%~dp0.venv\Scripts\python.exe' -ArgumentList 'worker.py' -WorkingDirectory '%~dp0' -WindowStyle Hidden"
timeout /t 2 /nobreak >nul
".venv\Scripts\python.exe" -m streamlit run app.py
pause
