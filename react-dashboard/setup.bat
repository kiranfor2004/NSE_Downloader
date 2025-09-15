@echo off
REM NSE React Dashboard Setup Script for Windows

echo === NSE React Dashboard Setup ===
echo.

REM Check if Node.js is installed
node --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Node.js is not installed
    echo 📋 Please install Node.js from: https://nodejs.org/
    echo    Recommended version: Node.js 18 or later
    echo.
    pause
    exit /b 1
)

REM Check if npm is available
npm --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ npm is not available
    echo 📋 Please ensure npm is installed with Node.js
    echo.
    pause
    exit /b 1
)

echo ✅ Node.js is installed
echo ✅ npm is available
echo.

REM Install dependencies
echo 📦 Installing dependencies...
npm install

if %errorlevel% equ 0 (
    echo.
    echo ✅ Dependencies installed successfully!
    echo.
    echo 🚀 Available commands:
    echo    npm run dev     - Start development server
    echo    npm run build   - Build for production
    echo    npm run preview - Preview production build
    echo.
    echo 📋 To start development:
    echo    1. Ensure the Flask API is running on http://localhost:5000
    echo    2. Run: npm run dev
    echo    3. Open: http://localhost:5173
    echo.
) else (
    echo.
    echo ❌ Installation failed!
    echo 📋 Please check the error messages above
    echo.
    pause
    exit /b 1
)

pause