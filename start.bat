@echo off
setlocal ENABLEDELAYEDEXPANSION
REM Cross-platform launcher for DocuSort on Windows.
REM - Creates .venv if missing
REM - Keeps Python dependencies up to date
REM - Warns about missing OCR binaries
REM - Refuses to start without config\config.yaml and .env

pushd "%~dp0"

set "PY=python"
where %PY% >nul 2>nul
if errorlevel 1 (
  set "PY=py -3"
  where py >nul 2>nul
  if errorlevel 1 (
    echo error: Python not found. Install Python 3.11+ from https://www.python.org/downloads/
    pause
    popd
    exit /b 1
  )
)

if not exist ".venv" (
  echo Creating virtual environment .venv
  %PY% -m venv .venv
  if errorlevel 1 (
    echo error: could not create virtual environment
    pause
    popd
    exit /b 1
  )
)

set "PIP=.venv\Scripts\pip.exe"
set "PYBIN=.venv\Scripts\python.exe"

echo Ensuring Python dependencies are up to date...
%PIP% install --quiet --upgrade pip
%PIP% install --quiet -r requirements.txt

where tesseract >nul 2>nul
if errorlevel 1 (
  echo WARN: tesseract.exe not found. Install from https://github.com/UB-Mannheim/tesseract/wiki
)

where ocrmypdf >nul 2>nul
if errorlevel 1 (
  echo WARN: ocrmypdf not found. Install with:  .venv\Scripts\pip install ocrmypdf
)

if not exist "config\config.yaml" (
  echo error: config\config.yaml is missing. Copy the template and adjust paths.
  pause
  popd
  exit /b 1
)

if not exist ".env" (
  echo error: .env is missing. Create with:  ANTHROPIC_API_KEY=sk-ant-...
  pause
  popd
  exit /b 1
)

REM Parse .env (lines of KEY=VALUE, blanks and #-lines ignored)
for /f "usebackq tokens=* delims=" %%L in (".env") do (
  set "LINE=%%L"
  if not "!LINE!"=="" if not "!LINE:~0,1!"=="#" (
    for /f "tokens=1,* delims==" %%A in ("!LINE!") do set "%%A=%%B"
  )
)

%PYBIN% -m docusort %*

popd
endlocal
