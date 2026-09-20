#!/bin/sh
set -e

# On first start, seed the mounted /app/config with the default files
# if the user hasn't placed their own there yet.
if [ ! -f /app/config/config.yaml ]; then
    echo "[docusort] Seeding default config to /app/config"
    mkdir -p /app/config
    cp -n /app/config-default/*.yaml /app/config/ 2>/dev/null || true
fi

exec "$@"
