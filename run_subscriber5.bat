@echo off
REM Easy Subscriber5 Launcher
REM Double-click this file to run the subscriber

echo ========================================
echo   AWS Kinesis Orderbook Subscriber
echo   Consuming from Kinesis Streams
echo ========================================
echo.

REM Add vcpkg DLLs to PATH
set PATH=%PATH%;C:\vcpkg\installed\x64-windows\bin

REM Navigate to project directory
cd /d "%~dp0"

echo Starting subscriber5...
echo Press Ctrl+C to stop
echo.

REM Run the subscriber
subscriber5.exe

pause
