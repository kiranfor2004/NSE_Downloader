@echo off
echo ========================================
echo NSE Delivery Analysis Dashboard 
echo Enhanced Three-Tab Version
echo ========================================
echo.
echo Starting Enhanced Dashboard API Server...
echo API will be available at: http://localhost:5001
echo Dashboard will be available at: http://localhost:5001
echo.

cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if virtual environment exists
if exist "..\\.venv\\Scripts\\python.exe" (
    echo Using virtual environment...
    ..\.venv\Scripts\python.exe api_dashboard.py
) else (
    echo Using system Python...
    python api_dashboard.py
)

pause