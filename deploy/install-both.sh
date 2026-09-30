#!/usr/bin/env bash
# DocuSort AND the Postwache, wired to each other — one command.
#
#   curl -fsSL https://raw.githubusercontent.com/robeertm/DocuSort/main/deploy/install-both.sh | bash
#
# Writes docker-compose.both.yml and an .env with a freshly made pairing secret,
# then starts both. Attachments from your mail land in DocuSort by themselves
# from the first minute: there is no account to create and no password to carry
# across.
#
# Re-running is safe — existing files are kept, only the images are refreshed.
set -euo pipefail

say()  { printf '\033[32m→\033[0m %s\n' "$*"; }
warn() { printf '\033[33m!\033[0m %s\n' "$*"; }
die()  { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

RAW="https://raw.githubusercontent.com/robeertm/DocuSort/main"
DIR="${INSTALL_DIR:-$PWD/docusort-postwache}"

command -v docker >/dev/null 2>&1 || die "Docker is missing. Install it and run this again."
COMPOSE="docker compose"
docker compose version >/dev/null 2>&1 || COMPOSE="docker-compose"
command -v "${COMPOSE%% *}" >/dev/null 2>&1 || die "Docker Compose is missing."

say "Installing into $DIR"
mkdir -p "$DIR/docusort/inbox" "$DIR/docusort/library" "$DIR/docusort/config" \
         "$DIR/docusort/logs" "$DIR/postwache/data"
cd "$DIR"

if [ -f docker-compose.both.yml ]; then
  warn "docker-compose.both.yml exists — keeping it."
else
  curl -fsSL "$RAW/docker-compose.both.yml" -o docker-compose.both.yml \
    || die "Could not download docker-compose.both.yml — no network?"
  say "Wrote docker-compose.both.yml"
fi

# ---------- The one secret both sides read ----------
# 🔴 Made HERE and never sent anywhere. It is the password of the account that
#    DocuSort creates for the Postwache, and it is the only thing that has to
#    match on both sides.
if grep -q '^PAIRING_SECRET=.' .env 2>/dev/null; then
  warn ".env already carries a PAIRING_SECRET — keeping it."
else
  if command -v openssl >/dev/null 2>&1; then
    SECRET="$(openssl rand -base64 24 | tr -d '\n/+=' )"
  else
    SECRET="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  fi
  [ -n "$SECRET" ] || die "Could not generate a secret."
  {
    echo "TZ=${TZ:-Europe/Berlin}"
    echo "PAIRING_SECRET=$SECRET"
    echo "COMPOSE_FILE=docker-compose.both.yml"
  } >> .env
  chmod 600 .env
  say "Wrote .env with a fresh pairing secret (chmod 600)"
fi

# 🔑 COMPOSE_FILE in .env means a plain `docker compose up -d` uses the both
#    file from now on — nobody has to remember a -f.
grep -q '^COMPOSE_FILE=' .env || echo "COMPOSE_FILE=docker-compose.both.yml" >> .env

say "Pulling images"
$COMPOSE pull
$COMPOSE up -d

HOST="$(hostname -I 2>/dev/null | awk '{print $1}')"
HOST="${HOST:-localhost}"
cat <<TXT

  Both are starting, and they already know each other.

    DocuSort     http://$HOST:9876     documents and household finances
    Postwache    http://$HOST:8110     watches your mailbox

  First visit to DocuSort asks you to create the admin account. Then tell the
  Postwache about your mailbox — and attachments start arriving on their own.

  Updates happen by themselves, nightly at 04:00.
  Over Tailscale instead of the LAN:  ./deploy/tailscale.sh tskey-auth-…
  Logs:  cd $DIR && $COMPOSE logs -f

TXT
