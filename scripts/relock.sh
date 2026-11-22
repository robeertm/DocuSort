#!/usr/bin/env bash
# Rebuild requirements.txt (exact versions + hashes) from requirements.in.
# Run with Python 3.12 (the image's Python); then run the tests and the gate.
#   scripts/relock.sh            # keep current pins where the ranges allow
#   scripts/relock.sh --upgrade  # lift every pin to the newest allowed version
set -euo pipefail
cd "$(dirname "$0")/.."
PIP_CONFIG_FILE=/dev/null pip-compile --generate-hashes --allow-unsafe --strip-extras -q \
  --index-url https://pypi.org/simple -o requirements.txt "$@" requirements.in
python3 scripts/check_pins.py
