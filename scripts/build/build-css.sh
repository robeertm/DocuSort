#!/usr/bin/env bash
# Compile Tailwind CSS into a static stylesheet served by FastAPI.
# Run from the repo root: ./scripts/build/build-css.sh
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"

if [ ! -x ./tailwindcss ]; then
  echo "Tailwind binary missing. Download from"
  echo "  https://github.com/tailwindlabs/tailwindcss/releases"
  echo "and place it at scripts/build/tailwindcss"
  exit 2
fi

OUT="$ROOT/docusort/web/static/tailwind.css"

./tailwindcss \
  -c ./tailwind.config.js \
  -i ./input.css \
  -o "$OUT" \
  --minify

echo "Wrote $OUT ($(wc -c <"$OUT" | tr -d ' ') bytes)"
