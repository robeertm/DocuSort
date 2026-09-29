#!/usr/bin/env bash
# Reach DocuSort over Tailscale — one command, one key.
#
#   ./deploy/tailscale.sh tskey-auth-xxxxxxxxxxxx
#
# That is the whole setup. The script puts the key into `.env`, remembers the
# Tailscale overlay there too (so a plain `docker compose up -d` keeps using
# it), starts everything and prints the address.
#
# Afterwards docusort is reachable at
#
#     https://docusort.<your-tailnet>.ts.net
#
# with a certificate Tailscale fetches and renews by itself. Nothing is exposed
# to the internet, no port forwarding, no reverse proxy, no certificate to look
# after. Everyone on your tailnet gets in; nobody else can, because there is
# nothing out there to reach.
#
# Run it again any time — it is idempotent, and it is also how you rotate the
# key.
set -euo pipefail

# 🔴 Heredocs that contain backticks or $ are quoted (<<'TXT'), because an
#    unquoted one EXECUTES what is in backticks and prints the result instead.
#    The one below that deliberately shows $COMPOSE is left unquoted.

say()  { printf '\033[32m→\033[0m %s\n' "$*"; }
warn() { printf '\033[33m!\033[0m %s\n' "$*"; }
die()  { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

RAW="https://raw.githubusercontent.com/robeertm/DocuSort/main"

# ---------- Where does this install live? ----------
# Either we were called from the directory holding docker-compose.yml, or from
# a clone — then it is the repository root, one level up from this script.
DIR="$PWD"
if [ ! -f "$DIR/docker-compose.yml" ]; then
  DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
[ -f "$DIR/docker-compose.yml" ] \
  || die "No docker-compose.yml found. Run this from the directory docusort is installed in."
cd "$DIR"

# ---------- The key ----------
KEY="${1:-${TS_AUTHKEY:-}}"
if [ -z "$KEY" ]; then
  cat <<'TXT'
Paste a Tailscale auth key. You get one here:

  Tailscale admin console → Settings → Keys → "Generate auth key"
  Switch on "Reusable" so a later restart does not need a new one.

TXT
  printf 'Auth key: '
  read -r KEY
fi
case "$KEY" in
  tskey-*) : ;;
  "")      die "No key given — nothing changed." ;;
  *)       warn "That does not look like a Tailscale key (they start with tskey-). Continuing anyway." ;;
esac

# ---------- The two files the overlay needs ----------
if [ ! -f docker-compose.tailscale.yml ]; then
  say "Fetching docker-compose.tailscale.yml"
  curl -fsSL "$RAW/docker-compose.tailscale.yml" -o docker-compose.tailscale.yml \
    || die "Could not download docker-compose.tailscale.yml — no network?"
fi
if [ ! -f tailscale/serve.json ]; then
  say "Fetching tailscale/serve.json"
  mkdir -p tailscale/state
  curl -fsSL "$RAW/tailscale/serve.json" -o tailscale/serve.json \
    || die "Could not download tailscale/serve.json — no network?"
fi
mkdir -p tailscale/state

# ---------- Write .env ----------
# 🔴 COMPOSE_FILE is the whole trick: with it in .env, a plain
#    `docker compose up -d` uses BOTH files. Without it you would have to
#    remember `-f docker-compose.yml -f docker-compose.tailscale.yml` every
#    single time — and the first time you forget, docusort comes up with its port
#    published and no tailnet, which looks like it worked.
touch .env
setenv() {                       # setenv KEY VALUE — replace or append
  local k="$1" v="$2" tmp
  tmp="$(mktemp)"
  grep -v "^${k}=" .env > "$tmp" 2>/dev/null || true
  printf '%s=%s\n' "$k" "$v" >> "$tmp"
  cat "$tmp" > .env
  rm -f "$tmp"
}
setenv TS_AUTHKEY "$KEY"
setenv COMPOSE_FILE "docker-compose.yml:docker-compose.tailscale.yml"
chmod 600 .env 2>/dev/null || true
say "Wrote TS_AUTHKEY and COMPOSE_FILE into .env (chmod 600)"

# ---------- Up ----------
COMPOSE="docker compose"
docker compose version >/dev/null 2>&1 || COMPOSE="docker-compose"
say "Starting — this also pulls the tailscale image the first time"
$COMPOSE up -d

# ---------- What is the address? ----------
# `tailscale cert` with no arguments prints the recommended domain in its usage
# text. Same trick as scripts/setup-tailscale-https.sh — no JSON parser needed.
say "Waiting for the tailnet (up to 60s)"
DOMAIN=""
for _ in $(seq 1 30); do
  DOMAIN="$($COMPOSE exec -T tailscale tailscale cert 2>&1 \
            | grep -oE '[a-z0-9_.-]+\.[a-z0-9-]+\.ts\.net' | head -1 || true)"
  [ -n "$DOMAIN" ] && break
  sleep 2
done

echo
if [ -n "$DOMAIN" ]; then
  printf '\033[32m✓\033[0m DocuSort is on your tailnet:\n\n    \033[1mhttps://%s\033[0m\n\n' "$DOMAIN"
  cat <<'TXT'
If the browser complains that the certificate does not exist, one switch is
still off — this is the only thing that cannot be done from here:

  Tailscale admin console → Settings → DNS
    • MagicDNS               on
    • HTTPS Certificates     on

Then run this script again (or just `docker compose restart tailscale`).
TXT
else
  warn "The container is up but has not reported a tailnet name yet."
  cat <<TXT
Look at what it says:

    $COMPOSE logs tailscale | tail -20

The usual causes: the auth key is used up (generate a REUSABLE one), or it
belongs to a different tailnet.
TXT
fi
