#!/usr/bin/env bash
# Double-clickable launcher for macOS Finder — delegates to start.sh.
# Keeps the Terminal window open so the user can read logs and shut down
# cleanly with Ctrl+C.

cd "$(dirname "$0")"
./start.sh "$@"

echo
echo "— DocuSort beendet. Dieses Fenster kannst du schließen."
read -r -p "Enter drücken, um das Fenster zu schließen… " _
