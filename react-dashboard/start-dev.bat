@echo off
REM Add Node.js to PATH for this session
set PATH=%PATH%;C:\Program Files\nodejs

REM Change to react dashboard directory
cd /d "c:\Users\kiran\NSE_Downloader\react-dashboard"

REM Start the development server
npm run dev

pause