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
# Set to 0 when a kept compose file publishes nothing; then there is no address
# to ask and the check below has to say so instead of pretending to measure.
VEROEFFENTLICHT=1
DIR="${DOCUSORT_DIR:-$PWD/docusort}"
PORT="${DOCUSORT_PORT:-9876}"
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

# 🔴 The port INSIDE the container is not ours to choose when an installation
# is already here. `config/config.yaml` lives in a mounted directory and is
# never overwritten (`docker-entrypoint.sh` seeds it with `cp -n`), so an
# installation from before 0.67.0 goes on listening on 8080 — and a compose
# file written with `:9876` would point at a door that is not there. Ask the
# config that is actually on disk; only a fresh installation gets 9876.
INNEN=9876
if [ -f "$DIR/config/config.yaml" ]; then
  GEFUNDEN="$(sed -n 's/^[[:space:]]*port:[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
              "$DIR/config/config.yaml" | head -1)"
  if [ -n "$GEFUNDEN" ] && [ "$GEFUNDEN" != "$INNEN" ]; then
    warn "this installation already listens on $GEFUNDEN inside the container — keeping that"
    INNEN="$GEFUNDEN"
  fi
fi

if [ -f "$DIR/docker-compose.yml" ]; then
  warn "docker-compose.yml exists — keeping it."
  # 🔴 Keeping it is right — it is the user's file. Keeping it SILENTLY is not:
  # a compose file without a published port starts a container that runs,
  # listens inside, and cannot be reached from anywhere. Nothing fails, nothing
  # is logged, and `docker compose ps` says `9876/tcp` instead of
  # `0.0.0.0:9876->9876/tcp` — which nobody reads as an error. This cost a user
  # an evening. The information was there; it was simply never said out loud.
  if ! grep -qE '^[[:space:]]*-[[:space:]]*"?[0-9$][^"]*:[0-9]+' "$DIR/docker-compose.yml"; then
    VEROEFFENTLICHT=0
    warn "🔴 but it publishes no port — DocuSort would be unreachable from outside."
    warn "   Put this next to it as docker-compose.override.yml and it is fixed"
    warn "   without touching your own file (compose merges the two):"
    printf '\n     services:\n       docusort:\n         ports:\n           - "%s:%s"\n\n' \
           "$PORT" "$INNEN"
  fi
else
  cat > "$DIR/docker-compose.yml" <<YAML
services:
  docusort:
    image: \${DOCUSORT_IMAGE}
    container_name: docusort
    restart: unless-stopped
    ports:
      - "\${DOCUSORT_PORT:-$PORT}:$INNEN"
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

# 🔴 WHY THIS BLOCK EXISTS
# Until now the installer ran `up -d`, printed „DocuSort is starting." and
# walked away. „Started" is Docker's word for „the process was launched", and a
# container that dies one second later and is restarted for ever says exactly
# the same thing. A user whose installation never came up was told it had, saw
# a never-green entry in his NAS interface, and had no way of knowing why —
# while the reason was sitting in the container's own log the whole time.
#
# So: wait until the web interface ANSWERS. If it does not, say so plainly and
# print what the container itself said.
hol() {                                   # 0 = something answered on that port
  if command -v curl >/dev/null 2>&1; then
    curl -fsS -o /dev/null -m 3 "$1" 2>/dev/null
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O /dev/null -T 3 "$1" 2>/dev/null
  else
    return 9                              # no way to ask — not a failure
  fi
}

# 🔴 Measured on real Docker, not assumed: `.State.RestartCount` does NOT
#    exist („map has no entry for key") — the counter sits at the TOP level,
#    while `Status` sits under `.State`. And a crash loop reports
#    Status=restarting, not exited.
zustand()  { docker inspect -f "{{.State.$1}}" docusort 2>/dev/null || true; }
neustarts() { docker inspect -f '{{.RestartCount}}' docusort 2>/dev/null || true; }

warte() {
  i=0
  while [ "$i" -lt 60 ]; do
    hol "http://127.0.0.1:$PORT/" && return 0
    [ $? -eq 9 ] && return 9
    # The restart loop first: it is the more precise diagnosis, and it is the
    # one failure that looks like a success in every interface — the container
    # is „running", again and again.
    n="$(neustarts)"
    case "$n" in ''|*[!0-9]*) n=0 ;; esac
    [ "$n" -ge 2 ] && return 3
    case "$(zustand Status)" in
      exited|dead) return 2 ;;            # it is gone — do not wait out the minute
    esac
    i=$((i + 1))
    sleep 1
  done
  return 1
}

if [ "$VEROEFFENTLICHT" = "0" ]; then
  warn "Skipping the check whether DocuSort answers: your compose file"
  warn "publishes no port, so there is no address to ask. Add the override"
  warn "shown above, then run this again."
else
  say "Waiting until DocuSort answers on port $PORT"
  set +e
  warte
  ERG=$?
  set -e
  case "$ERG" in
    0) say "DocuSort answers on port $PORT." ;;
    9) warn "Neither curl nor wget is here, so this cannot be checked." ;;
    *)
      printf '\n'
      case "$ERG" in
        2) warn "🔴 The container has stopped again. DocuSort is NOT running." ;;
        3) warn "🔴 The container keeps restarting ($(neustarts) times so far)."
           warn "   That is why your Docker interface shows it as starting and never green." ;;
        *) warn "🔴 DocuSort did not answer within 60 seconds." ;;
      esac
      printf '\n'
      warn "What DocuSort itself said — this is the actual reason:"
      printf '\n'
      ( cd "$DIR" && $COMPOSE logs --tail 30 docusort 2>&1 ) | sed 's/^/    /'
      printf '\n'
      warn "The whole log:  cd $DIR && $COMPOSE logs -f docusort"
      warn "Nothing above is lost — fix the cause and run this installer again."
      exit 1
      ;;
  esac
fi

# `hostname -I` is Linux-only, and with `set -euo pipefail` a failing one took
# the whole script down HERE — after the container was already up. The install
# had worked and the last thing the user saw was a non-zero exit and no
# address, no paths, no next step.
HOST="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
HOST="${HOST:-localhost}"
cat <<DONE

  DocuSort is running.

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
