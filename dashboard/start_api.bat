@echo off
echo Starting NSE Dashboard API...
cd /d "C:\Users\kiran\NSE_Downloader\dashboard"
python api.py
if errorlevel 1 (
    echo Error starting API server
    pause
) else (
    echo API server stopped
    pause
)