#!/usr/bin/env bash
# DocuSort — one-command install.
#
#   curl -fsSL https://raw.githubusercontent.com/robeertm/DocuSort/main/deploy/install.sh | bash
#
# Writes a docker-compose.yml plus an .env into ./docusort (or $DOCUSORT_DIR),
# pulls the published image and starts it. Re-running it is safe: existing
# files are kept and only the image is refreshed.
set -euo pipefail

IMAGE="${DOCUSORT_IMAGE:-ghcr.io/robeertm/docusort:latest}"
DIR="${DOCUSORT_DIR:-$PWD/docusort}"
PORT="${DOCUSORT_PORT:-8080}"
TZ_DEFAULT="${TZ:-$(readlink /etc/localtime 2>/dev/null | sed 's#.*/zoneinfo/##')}"
TZ_DEFAULT="${TZ_DEFAULT:-Europe/Berlin}"

say()  { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "Docker is not installed. See https://docs.docker.com/get-docker/"
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  die "Docker Compose is missing. Install the compose plugin and run this again."
fi
docker info >/dev/null 2>&1 || die "Docker is installed but not running (or this user may not talk to it)."

say "Installing into $DIR"
mkdir -p "$DIR/data/inbox" "$DIR/data/library" "$DIR/config"

if [ -f "$DIR/docker-compose.yml" ]; then
  warn "docker-compose.yml exists — keeping it."
else
  cat > "$DIR/docker-compose.yml" <<YAML
services:
  docusort:
    image: \${DOCUSORT_IMAGE}
    container_name: docusort
    restart: unless-stopped
    ports:
      - "\${DOCUSORT_PORT}:8080"
    environment:
      - TZ=\${TZ}
      - DOCUSORT_LOG_LEVEL=INFO
      # Optional: put your key here instead of typing it into the settings page.
      # - ANTHROPIC_API_KEY=\${ANTHROPIC_API_KEY}
    volumes:
      - ./data:/data
      - ./config:/app/config
      - ./logs:/app/logs
YAML
  say "Wrote $DIR/docker-compose.yml"
fi

if [ -f "$DIR/.env" ]; then
  warn ".env exists — keeping it."
else
  cat > "$DIR/.env" <<ENV
DOCUSORT_IMAGE=$IMAGE
DOCUSORT_PORT=$PORT
TZ=$TZ_DEFAULT
ENV
  say "Wrote $DIR/.env"
fi

say "Pulling $IMAGE"
( cd "$DIR" && $COMPOSE pull && $COMPOSE up -d )

HOST="$(hostname -I 2>/dev/null | awk '{print $1}')"
HOST="${HOST:-localhost}"
cat <<DONE

  DocuSort is starting.

    Web UI        http://$HOST:$PORT
    Documents     $DIR/data/library
    Drop scans in $DIR/data/inbox
    Config        $DIR/config/config.yaml

  The first visit asks you to create the admin account.
  Then open Settings and choose your AI provider.

  Update later:  cd $DIR && $COMPOSE pull && $COMPOSE up -d
  Logs:          cd $DIR && $COMPOSE logs -f

DONE
