@echo off
REM Run Both Publisher and Subscriber
REM This will open 2 terminal windows

echo ========================================
echo   Starting Publisher and Subscriber
echo ========================================
echo.

echo Opening Publisher in new window...
start "Publisher - AWS Kinesis" cmd /k "%~dp0run_publisher.bat"

echo Waiting 3 seconds...
timeout /t 3 /nobreak > nul

echo Opening Subscriber5 in new window...
start "Subscriber5 - AWS Kinesis" cmd /k "%~dp0run_subscriber5.bat"

echo.
echo Both windows opened!
echo Close this window or press any key to exit.
pause > nul
