#!/usr/bin/env bash
# Cross-platform launcher for DocuSort (Linux / macOS).
# - Creates a .venv if missing
# - Keeps Python dependencies up to date
# - Warns about missing OCR binaries (tesseract / ocrmypdf)
# - Refuses to start without config/config.yaml and .env

set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"

if ! command -v "$PY" >/dev/null 2>&1; then
  echo "error: $PY not found. Install Python 3.11 or newer first." >&2
  echo "  macOS:  brew install python@3.13" >&2
  echo "  Debian: sudo apt install python3 python3-venv" >&2
  exit 1
fi

if [ ! -d .venv ]; then
  echo "→ creating virtual environment (.venv)"
  "$PY" -m venv .venv
fi

PIP=".venv/bin/pip"
PYBIN=".venv/bin/python"

echo "→ ensuring Python dependencies are up to date"
"$PIP" install --quiet --upgrade pip
"$PIP" install --quiet -r requirements.txt

for tool in tesseract ocrmypdf; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "⚠ warning: $tool not found on PATH"
    case "$tool" in
      tesseract)  echo "   macOS:  brew install tesseract tesseract-lang"
                  echo "   Debian: sudo apt install tesseract-ocr tesseract-ocr-deu tesseract-ocr-eng" ;;
      ocrmypdf)   echo "   macOS:  brew install ocrmypdf"
                  echo "   Debian: sudo apt install ocrmypdf ghostscript qpdf pngquant unpaper" ;;
    esac
  fi
done

if [ ! -f config/config.yaml ]; then
  echo "error: config/config.yaml is missing. Copy config/config.yaml from the repo and adjust the paths." >&2
  exit 1
fi

if [ ! -f .env ]; then
  echo "error: .env is missing. Create it with at least:" >&2
  echo "  ANTHROPIC_API_KEY=sk-ant-..." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

exec "$PYBIN" -m docusort "$@"
