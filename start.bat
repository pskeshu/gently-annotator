@echo off
REM Launch the Gently Annotator server.
REM
REM Usage:
REM   start.bat               REM uses host/port from config.yaml
REM   start.bat --port 8091   REM extra args pass through to annotator.server
REM
REM Reads the venv at .\venv\. If it doesn't exist, exits with a hint on
REM how to bootstrap it.

setlocal
set REPO=%~dp0
set PY=%REPO%venv\Scripts\python.exe

if not exist "%PY%" (
    echo venv not found at: %PY%
    echo.
    echo Bootstrap it once with:
    echo   python -m venv venv
    echo   venv\Scripts\python.exe -m pip install -e .
    exit /b 1
)

cd /d "%REPO%"
"%PY%" -m annotator.server %*
