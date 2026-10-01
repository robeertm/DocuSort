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
if [ ! -f "$DIR/docker-compose.yml" ] && [ ! -f "$DIR/docker-compose.both.yml" ]; then
  DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
cd "$DIR"

# 🔑 Zwei Gestalten, ein Skript. Eine Doppelinstallation (DocuSort + Postwache)
#    braucht zwei Beiwagen und zwei serve-Dateien, damit BEIDE einen eigenen
#    Namen bekommen statt einer von beiden eine Portnummer in der Adresse.
if [ -f docker-compose.both.yml ]; then
  BASIS="docker-compose.both.yml"
  UEBERLAGERUNG="docker-compose.both.tailscale.yml"
  SERVE="tailscale/serve-docusort.json tailscale/serve-postwache.json"
  ZUSTAND="tailscale/state-docusort tailscale/state-postwache"
  BEIWAGEN="ts-docusort ts-postwache"
elif [ -f docker-compose.yml ]; then
  BASIS="docker-compose.yml"
  UEBERLAGERUNG="docker-compose.tailscale.yml"
  SERVE="tailscale/serve.json"
  ZUSTAND="tailscale/state"
  BEIWAGEN="tailscale"
else
  die "No docker-compose.yml and no docker-compose.both.yml here. Run this in the directory the install lives in."
fi
say "Found $BASIS"

# ---------- The key ----------
KEY="${1:-${TS_AUTHKEY:-}}"
if [ -z "$KEY" ]; then
  cat <<'TXT'
Paste a Tailscale auth key. You get one here:

  Tailscale admin console → Settings → Keys
    → under "Auth keys", the button "Generate auth key..."
      Switch on "Reusable" so a later restart does not need a new one.

  🔴 NOT "Generate access token..." further down that page. That one is a key
     for the Tailscale API and cannot log a machine in. The right one starts
     with  tskey-auth-

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
if [ ! -f "$UEBERLAGERUNG" ]; then
  say "Fetching $UEBERLAGERUNG"
  curl -fsSL "$RAW/$UEBERLAGERUNG" -o "$UEBERLAGERUNG" \
    || die "Could not download $UEBERLAGERUNG — no network?"
fi
mkdir -p tailscale
for f in $SERVE; do
  [ -f "$f" ] && continue
  say "Fetching $f"
  curl -fsSL "$RAW/$f" -o "$f" || die "Could not download $f — no network?"
done
mkdir -p $ZUSTAND

# 🔴 DIE STILLE FALLE AN DIESER STELLE
# `serve.json` trägt eine feste Zahl: 9876, DocuSorts Vorgabe seit 0.67.0. Eine
# Installation von davor hört im Container weiter auf 8080 (`config.yaml`
# gehört dem Nutzer und wird nie überschrieben). Dann legt Tailscale 443 auf
# 127.0.0.1:9876 — wo niemand horcht. Der Name löst auf, das Zertifikat stimmt,
# und die Seite antwortet mit einem Fehler, der nach Tailscale aussieht und
# keiner ist.
# Also dieselbe Frage wie im Installer: welchen Port sagt die config, die
# WIRKLICH auf der Platte liegt?
INNEN=""
if [ -f config/config.yaml ]; then
  INNEN="$(sed -n 's/^[[:space:]]*port:[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
           config/config.yaml | head -1)"
fi
if [ -n "$INNEN" ] && [ -f tailscale/serve.json ]; then
  ALT="$(sed -n 's#.*127\.0\.0\.1:\([0-9][0-9]*\).*#\1#p' tailscale/serve.json | head -1)"
  if [ -n "$ALT" ] && [ "$ALT" != "$INNEN" ]; then
    say "This installation listens on $INNEN inside the container — pointing Tailscale there"
    tmp="$(mktemp)"
    sed "s#127\.0\.0\.1:$ALT#127.0.0.1:$INNEN#" tailscale/serve.json > "$tmp"
    cat "$tmp" > tailscale/serve.json
    rm -f "$tmp"
  fi
fi

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
setenv COMPOSE_FILE "$BASIS:$UEBERLAGERUNG"
chmod 600 .env 2>/dev/null || true
say "Wrote TS_AUTHKEY and COMPOSE_FILE into .env (chmod 600)"

# ---------- Up ----------
COMPOSE="docker compose"
docker compose version >/dev/null 2>&1 || COMPOSE="docker-compose"
say "Starting — this also pulls the tailscale image the first time"
# 🔴 WARUM DAS NICHT EINFACH `$COMPOSE up -d` BLEIBT
# Startet ein Beiwagen nicht — ein verbrauchter Auth-Key reicht dafuer —, dann
# koennen die Anwendungen seine Netzwerk-Umgebung nicht betreten, und Docker
# sagt dazu:
#     cannot join network namespace of container …: is restarting
# Das ist wahr und voellig unbrauchbar. Der Grund steht im Log des Beiwagens,
# also wird er geholt und hingeschrieben, statt den Nutzer damit allein zu
# lassen.
if ! $COMPOSE up -d; then
  echo
  warn "Something did not come up. What the Tailscale side says:"
  echo
  for b in $BEIWAGEN; do
    printf '  ── %s ──\n' "$b"
    L="$($COMPOSE logs --tail 15 "$b" 2>&1 || true)"
    # 🔑 Ein leeres Log ist eine Aussage, kein Nichts — sagen, statt eine
    #    Leerzeile zu drucken und den Leser raten zu lassen.
    if [ -z "${L//[[:space:]]/}" ]; then
      printf '    (it said nothing at all — it did not get as far as a message)\n'
    else
      printf '%s\n' "$L" | sed 's/^/    /'
    fi
  done
  echo
  cat <<'TXT'
  The usual cause is the auth key: used up (generate a REUSABLE one), expired,
  or belonging to a different tailnet. Generate a new one here and run this
  again — nothing else has to be undone:

    Tailscale admin console → Settings → Keys
      → under "Auth keys": "Generate auth key..."   (NOT "access token")
      • Reusable   on
TXT
  exit 1
fi

# „Started" ist nicht „laeuft". Ein Beiwagen, der sich im Kreis dreht, meldet
# genau dasselbe — dieselbe Falle wie beim Installer.
sleep 4
for b in $BEIWAGEN; do
  ZUSTAND="$(docker inspect -f '{{.State.Status}}' "$b" 2>/dev/null || echo weg)"
  case "$ZUSTAND" in
    running) : ;;
    *)
      warn "The Tailscale container $b is $ZUSTAND. Its own words:"
      $COMPOSE logs --tail 15 "$b" 2>&1 | sed 's/^/    /'
      die "Fix the key (REUSABLE, not expired) and run this again."
      ;;
  esac
done

# ---------- What is the address? ----------
# `tailscale cert` with no arguments prints the recommended domain in its usage
# text. Same trick as scripts/setup-tailscale-https.sh — no JSON parser needed.
say "Waiting for the tailnet (up to 60s)"
DOMAIN=""
GEFUNDEN=""
for _ in $(seq 1 30); do
  DOMAIN=""
  for b in $BEIWAGEN; do
    d="$($COMPOSE exec -T "$b" tailscale cert 2>&1 \
         | grep -oE '[a-z0-9_.-]+\.[a-z0-9-]+\.ts\.net' | head -1 || true)"
    [ -n "$d" ] && DOMAIN="$DOMAIN $d"
  done
  GEFUNDEN="$(printf '%s' "$DOMAIN" | tr ' ' '\n' | grep -c . || true)"
  ZAHL="$(printf '%s' "$BEIWAGEN" | tr ' ' '\n' | grep -c .)"
  [ "${GEFUNDEN:-0}" -ge "$ZAHL" ] && break
  sleep 2
done

echo
if [ -n "$DOMAIN" ]; then
  printf '\033[32m✓\033[0m On your tailnet:\n\n'
  for d in $DOMAIN; do printf '    \033[1mhttps://%s\033[0m\n' "$d"; done
  printf '\n'
  printf '  Open those on the phone and add them to the home screen — done.\n\n'
  cat <<'TXT'
If the browser complains that the certificate does not exist, one switch is
still off — this is the only thing that cannot be done from here:

  Tailscale admin console → Settings → DNS
    • MagicDNS               on
    • HTTPS Certificates     on

Then run this script again.
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
