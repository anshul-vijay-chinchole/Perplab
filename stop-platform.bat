@echo off
rem Stop the PerpLab server — and ONLY the server.
rem
rem Found by the port it listens on (8756), never by process name: the data collector is
rem also a python.exe on this machine and killing by name would take down an unattended
rem recording run that has nothing to do with the UI.
rem
rem Stopping the server does not stop running paper/live sessions cleanly — stop those
rem from the UI first if any are running.

setlocal enabledelayedexpansion
set FOUND=
for /f "tokens=5" %%p in ('netstat -ano ^| findstr /r ":8756 .*LISTENING"') do (
    set FOUND=1
    echo Stopping PerpLab server (pid %%p^)
    taskkill /pid %%p /f >nul 2>&1
)
if not defined FOUND echo PerpLab server is not running.
pause
