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
# Every host path the compose file below mounts, created before the daemon is
# asked for it. `logs` was missing here and the installer still stopped on some
# machines and not others: most Docker daemons create a missing bind-mount
# source themselves (as root), Synology's refuses and answers
#   Error response from daemon: Bind mount failed: '…/logs' does not exist
# — so the omission only ever showed up on a NAS, which is where a lot of
# people put this.
mkdir -p "$DIR/data/inbox" "$DIR/data/library" "$DIR/config" "$DIR/logs"

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

  # Keeps itself up to date, nightly at 04:00. \`image: …:latest\` is a label,
  # not a subscription — Docker never re-pulls a running container, so without
  # this you would have to remember it yourself.
  # It needs the Docker socket (effectively root on the host, mounted
  # read-only) and it watches ONLY the docusort container. Delete this service
  # to switch it off; put WATCHTOWER_SCHEDULE=... in .env to move the time.
  watchtower:
    image: ghcr.io/nicholas-fedor/watchtower:latest
    container_name: docusort-watchtower
    restart: unless-stopped
    environment:
      - TZ=\${TZ}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    command:
      - --cleanup
      - --schedule
      - \${WATCHTOWER_SCHEDULE:-0 0 4 * * *}
      - docusort
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

# The list above is what THIS installer mounts. A compose file that was
# already here can mount paths of its own, so read them back out of the file
# that is actually going to be used and make anything still missing. Then a
# future omission costs a line of output instead of an install.
while IFS= read -r rel; do
  case "$rel" in *.*) continue ;; esac      # a file mount is the user's to provide
  [ -d "$DIR/$rel" ] || { warn "creating missing mount directory ./$rel"; mkdir -p "$DIR/$rel"; }
done < <(sed -n 's#^[[:space:]]*-[[:space:]]*\./\([^:]*\):.*#\1#p' "$DIR/docker-compose.yml" | sort -u)

say "Pulling $IMAGE"
( cd "$DIR" && $COMPOSE pull && $COMPOSE up -d )

# `hostname -I` is Linux-only, and with `set -euo pipefail` a failing one took
# the whole script down HERE — after the container was already up. The install
# had worked and the last thing the user saw was a non-zero exit and no
# address, no paths, no next step.
HOST="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
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
                 (or uncomment the watchtower block in docker-compose.yml
                  to have it done for you)
  Logs:          cd $DIR && $COMPOSE logs -f

DONE
