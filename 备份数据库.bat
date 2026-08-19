@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
".venv\Scripts\python.exe" -c "from db import init_db,backup_database; init_db(); print('备份完成:', backup_database())"
pause
