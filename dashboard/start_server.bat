@echo off
cd /d "C:\Users\kiran\NSE_Downloader\dashboard"
echo Starting NSE Dashboard API Server...
echo.
echo Dashboard will be available at: http://localhost:5000
echo Press Ctrl+C to stop the server
echo.
python api.py
pause