#!/usr/bin/env bash
# Deploy the current working tree to the shelly-energy-analyzer VM.
#
# Pushes code, skips .env and the VM-specific config.yaml on purpose,
# then prints the one-liner you need to paste to restart the service
# (systemctl restart needs sudo, so the script does not run it for you).

set -euo pipefail

HOST="${DOCUSORT_VM_HOST:-robeertm@shelly-energy-analyzer}"
LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "→ rsync → $HOST:~/docusort/"
rsync -avz \
  --exclude='.venv' \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='.env' \
  --exclude='config/config.yaml' \
  --exclude='scripts/deploy-vm.sh' \
  --exclude='scripts/build/tailwindcss' \
  "$LOCAL_DIR/" "$HOST:~/docusort/"

echo
echo "→ reinstalling Python deps (in case requirements.txt changed)"
ssh "$HOST" 'cd ~/docusort && .venv/bin/pip install -q -r requirements.txt'

echo
echo "── Next step: paste this on the VM to restart the service:"
echo
echo 'sudo systemctl restart docusort && sleep 3 && sudo systemctl status docusort --no-pager | head -10 && curl -s -o /dev/null -w "HTTP %{http_code}  /\n" http://localhost:9876/'

if command -v pbcopy >/dev/null 2>&1; then
  printf 'sudo systemctl restart docusort && sleep 3 && sudo systemctl status docusort --no-pager | head -10 && curl -s -o /dev/null -w "HTTP %%{http_code}  /\\n" http://localhost:9876/\n' | pbcopy
  echo
  echo "   (copied to clipboard)"
fi
