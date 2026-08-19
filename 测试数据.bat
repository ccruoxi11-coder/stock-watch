@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo 请先运行“启动盯盘.bat”创建运行环境。
  pause
  exit /b 1
)

echo 正在测试行情接口...
".venv\Scripts\python.exe" test_data.py
pause
