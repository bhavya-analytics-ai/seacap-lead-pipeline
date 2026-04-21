@echo off
setlocal enabledelayedexpansion
set INSTALL_DIR=C:\SeaCap\lead-pipeline
set PYTHON_EXE=%INSTALL_DIR%\lead_pipeline\Scripts\python.exe
set WATCHER_SCRIPT=%INSTALL_DIR%\scripts\watch_incoming.py

:: Pull latest code
cd /d "%INSTALL_DIR%"
git pull >nul 2>&1

:: Install any new packages silently
"%PYTHON_EXE%" -m pip install -q -r requirements.txt >nul 2>&1

:: Restart watcher — kill existing then start fresh
for /f "tokens=2" %%p in ('tasklist /fi "IMAGENAME eq python.exe" /fo list 2^>nul ^| findstr "PID:"') do (
    wmic process where "ProcessId=%%p" get CommandLine 2^>nul | findstr /i "watch_incoming" >nul 2>&1
    if not errorlevel 1 taskkill /f /pid %%p >nul 2>&1
)
timeout /t 2 /nobreak >nul
start "SeaCap Lead Pipeline" /min "%PYTHON_EXE%" "%WATCHER_SCRIPT%"
