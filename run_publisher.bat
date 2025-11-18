@echo off
REM Easy Publisher Launcher
REM Double-click this file to run the publisher

echo ========================================
echo   Multi-Exchange Orderbook Publisher
echo   Publishing to AWS Kinesis
echo ========================================
echo.

REM Add vcpkg DLLs to PATH
set PATH=%PATH%;C:\vcpkg\installed\x64-windows\bin

REM Navigate to publisher directory
cd /d "%~dp0Distributive-Processing-for-Websocket\publisher_cpp"

echo Starting publisher...
echo Press Ctrl+C to stop
echo.

REM Run the publisher
orderbook_rt.exe

pause
