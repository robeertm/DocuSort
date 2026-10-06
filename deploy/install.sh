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
# Der Weg, auf dem die meisten hier ankommen — gebraucht fuer Hinweise, die
# dem Menschen sagen, wie er diesen Installer noch einmal startet.
EINZEILER="https://raw.githubusercontent.com/robeertm/DocuSort/main/deploy/install.sh"
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

# 🔴 ZWEI DocuSorts auf einer Maschine
# Ein Nutzer hatte neben dem Container dieses Installers noch einen zweiten aus
# demselben Abbild (`ghcr-io-robeertm-docusort-1`, aus der NAS-Oberflaeche
# angelegt). Der eine lief gruen, der andere drehte sich ewig im Kreis — und
# nichts auf dem Schirm sagte, dass es zwei sind. Beide wollen denselben
# Wirts-Port, beide haengen an ihren eigenen Daten: man richtet den einen ein
# und ruft den anderen auf. Also nachsehen und es SAGEN, bevor ein zweiter
# dazukommt. Gemessen: `{{.Image}}` druckt den Namen genau so, wie er hier steht.
ANDERE="$(docker ps -a --format '{{.Names}}\t{{.Image}}' 2>/dev/null \
          | awk -F'\t' -v img="$IMAGE" '$2==img && $1!="docusort" {print $1}' || true)"
if [ -n "$ANDERE" ]; then
  warn "🔴 This machine already runs DocuSort in another container:"
  for n in $ANDERE; do warn "     $n"; done
  warn "   Two of them fight over the same port and keep separate data —"
  warn "   you would set one up and open the other. Keep ONE."
  warn "   Look at it first:  docker inspect $(echo "$ANDERE" | head -1) \\"
  warn "                        --format '{{.HostConfig.PortBindings}} {{range .Mounts}}{{.Source}} {{end}}'"
  warn "   Then remove the one you do not want:  docker rm -f <name>"
  warn "   Nothing has been changed. Run this installer again afterwards."
  exit 1
fi


# ══════════════════════════════════════════════════════════════════════════
# 🔴 WO GEHOERT DIESE INSTALLATION HIN? (06.10.2026)
#
# Gemeldet: nach einer fehlerhaften Fassung und dem Update danach kam ein
# Nutzer nicht mehr an seine Datenbank und hat alle Dokumente neu hochgeladen.
# Nachgestellt und gemessen, und es stimmt — nur war nichts verloren:
#
#   `DIR` hing am ARBEITSVERZEICHNIS (`$PWD/docusort`), und die erzeugte
#   compose-Datei mountet RELATIV (`./data:/data`). Wer den Einzeiler ein
#   zweites Mal laufen laesst und dabei woanders steht — im Download-Ordner,
#   im Heimatverzeichnis, in der Installation selbst —, bekam ein ZWEITES
#   Verzeichnis. Der Container heisst in beiden Faellen `docusort`, also baute
#   compose ihn mit den NEUEN Pfaden neu: DocuSort lief, die Bibliothek war
#   leer, und die alte Datenbank lag unversehrt im anderen Ordner.
#   Gemessen: Rueckgabe 0, KEIN Wort dazu.
#
# 🔑 DIE KUR IST NICHT EINE WARNUNG, SONDERN DER RICHTIGE ORDNER.
#    Das Arbeitsverzeichnis ist die schlechteste aller Quellen — es sagt nur,
#    wo jemand zufaellig stand. Es gibt bessere, und sie werden der Reihe nach
#    gefragt:
#
#      1. `DOCUSORT_DIR` — ausdruecklich gesagt gewinnt immer.
#      2. der LAUFENDE Container: wo liegen SEINE Daten? Das ist die
#         Installation, die dieser Rechner benutzt.
#      3. steht man MITTEN DRIN? Dann ist es dieser Ordner, nicht ein
#         `docusort/docusort` darunter.
#      4. liegt daneben schon eine? Dann die.
#      5. sonst: eine neue, hier.
#
#    Danach wird noch gefragt, ob irgendwo eine Datenbank liegt, die NIEMAND
#    benutzt — der Zustand, in dem der Nutzer stand. Verschoben wird dabei
#    nichts: ein Installer, der die Dokumente eines Menschen umraeumt, ist
#    schlimmer als das Problem. Er zeigt sie, und wenn das Ziel leer ist und es
#    genau EINE gibt, richtet er sich nach ihr.
# ══════════════════════════════════════════════════════════════════════════

# Sieht dieser Ordner nach einer DocuSort-Installation aus?
ist_installation() {
  [ -n "${1:-}" ] || return 1
  [ -f "$1/docker-compose.yml" ] && grep -q "docusort" "$1/docker-compose.yml" 2>/dev/null && return 0
  [ -f "$1/data/library/docusort.db" ] && return 0
  return 1
}

# Wo liegen die Daten des laufenden Containers? (Leer, wenn es keinen gibt.)
LAUFEND_DATEN="$(docker inspect docusort \
                 --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' \
                 2>/dev/null || true)"
LAUFEND_DIR=""
if [ -n "$LAUFEND_DATEN" ]; then
  _k="$(dirname "$LAUFEND_DATEN")"
  # 🔴 Nur uebernehmen, wenn daneben wirklich eine compose-Datei liegt. Bei
  #    einem benannten Volume zeigt `.Source` nach /var/lib/docker, und dessen
  #    Elternordner ist keine Installation.
  [ -f "$_k/docker-compose.yml" ] && LAUFEND_DIR="$_k"
fi

GRUND=""
if [ -n "${DOCUSORT_DIR:-}" ]; then
  GRUND="you said so (DOCUSORT_DIR)"
elif [ -n "$LAUFEND_DIR" ]; then
  DIR="$LAUFEND_DIR"
  GRUND="this is where the running DocuSort keeps its documents"
elif ist_installation "$PWD"; then
  # 🔴 Genau hier entstand `docusort/docusort`: wer IN seiner Installation
  #    steht und den Einzeiler noch einmal startet, meint diese.
  DIR="$PWD"
  GRUND="you are standing in it"
elif ist_installation "$PWD/docusort"; then
  DIR="$PWD/docusort"
  GRUND="it is already here"
fi

# 🔑 Eine Datenbank, die niemand benutzt. Gesucht wird in den Ordnern, in denen
#    Menschen wirklich stehen, zwei Ebenen tief — kein Durchsuchen der Platte.
WAISEN=""
for _ort in "$PWD" "$PWD/.." "${HOME:-/nonexistent}" "$(dirname "$DIR")" \
            /volume1/docker /opt /srv; do
  [ -d "$_ort" ] || continue
  for _k in "$_ort"/data/library/docusort.db "$_ort"/*/data/library/docusort.db \
            "$_ort"/*/*/data/library/docusort.db; do
    [ -f "$_k" ] || continue
    _d="$(cd "$(dirname "$_k")/../.." 2>/dev/null && pwd)" || continue
    [ "$_d" = "$DIR" ] && continue
    [ "$_d/data" = "$LAUFEND_DATEN" ] && continue
    case " $WAISEN " in *" $_d "*) continue ;; esac
    WAISEN="$WAISEN $_d"
  done
done
WAISEN="$(printf '%s' "$WAISEN" | sed 's/^ *//')"

if [ -n "$WAISEN" ]; then
  _anzahl=0
  for _w in $WAISEN; do _anzahl=$((_anzahl + 1)); done
  warn "🔴 There is a DocuSort database here that nothing is using:"
  for _w in $WAISEN; do
    warn "     $_w/data/library/docusort.db  ($(du -h "$_w/data/library/docusort.db" 2>/dev/null | cut -f1), last written $(date -r "$_w/data/library/docusort.db" '+%Y-%m-%d %H:%M' 2>/dev/null))"
  done
  # 🔴 UND ES WIRD NICHT VON SELBST DORTHIN UMGEZOGEN.
  #
  # Der erste Entwurf tat genau das: Ziel leer, genau ein Fund — also
  # „installing there instead". In der Simulation hat das sofort zugeschlagen,
  # und zwar falsch: eine FRISCHE Installation im Heimatverzeichnis fand eine
  # Datenbank im Download-Ordner und verlegte sich dorthin. Niemand hatte
  # danach gefragt, und auf dem Schirm stand ein Pfad, den der Mensch nie
  # genannt hatte.
  #
  # 🔑 Gezeigt wird es, entschieden wird es nicht. Wer es benutzen will, sagt
  #    es mit DOCUSORT_DIR — und wer beide Haelften zusammen haben will,
  #    bekommt weiter unten das Angebot, sie zu vereinigen. Das ist die
  #    Korrektur, die nichts ueberrascht.
  warn "   Not touched — this run installs into $DIR."
  # 🔴 NICHT `$0`. Dieses Skript laeuft im Normalfall als `curl … | bash` —
  #    dann ist `$0` schlicht `bash`, und auf dem Schirm stuende ein Befehl,
  #    der nichts tut. Der Weg, auf dem der Mensch hergekommen ist, steht
  #    oben im Kopf dieser Datei; genau der gehoert hierher.
  warn "   To use that one instead, run the installer with it named:"
  warn "     DOCUSORT_DIR=<folder> bash -c \"\$(curl -fsSL $EINZEILER)\""
  warn ""
fi

if [ -n "$GRUND" ]; then
  say "Using $DIR — $GRUND"
else
  say "Fresh installation in $DIR"
fi

# 🔴 Ein zweiter Container waere ein zweiter Haushalt. Wer wirklich einen
#    will, sagt es mit DOCUSORT_DIR; ohne das richtet sich der Installer nach
#    oben aus und aktualisiert die vorhandene Installation.
if [ -n "$LAUFEND_DIR" ] && [ "$LAUFEND_DIR" != "$DIR" ]; then
  warn "A DocuSort is already running from $LAUFEND_DIR."
  warn "   This run uses $DIR instead, so the container will be rebuilt there"
  warn "   and the other folder keeps its documents untouched."
fi

# 🔑 Was mit den gefundenen Archiven geschieht, entscheidet sich NACH der
#    Installation — vorher laeuft noch nichts, was sie zusammenfuehren
#    koennte. Der Merker traegt sie bis dorthin.
ZUSAMMEN="$WAISEN"

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

  # Keeps itself up to date, once an hour. \`image: …:latest\` is a label,
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
      # 🔴 OHNE DIESE FLAGGE KOMMT EINE KAPUTTE FASSUNG NIE ZURUECK.
      # 🔴 Die Zeichen um die Woerter unten sind ABSICHTLICH einfache
      #    Anfuehrungszeichen. Dieser Block ist ein Heredoc OHNE Schutz
      #    (<<YAML, nicht <<'YAML') — die Dollarzeichen darin sollen ja
      #    maskiert durchgehen. Damit sind Backticks KOMMANDOS: beim Lauf
      #    stand auf dem Schirm von Nutzern
      #        install.sh: line 100: restarting: command not found
      #    waehrend die Worte aus der erzeugten Datei verschwanden.
      #    Watchtower sieht Container im Zustand 'restarting' sonst GAR
      #    NICHT — am Pi gemessen: laufend 'scanned=1, updated=1',
      #    abstuerzend 'scanned=0'. Startet DocuSort nach einem Update
      #    nicht mehr, bliebe es stehen, bis jemand von Hand eingreift.
      #    Mit der Flagge holt der naechste Lauf die heile Fassung.
      - --include-restarting
      - --schedule
      - \${WATCHTOWER_SCHEDULE:-0 0 * * * *}
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

# 🔑 GESAGT, BEVOR ES PASSIERT. Mit dem Abbild kommt ein lokaler KI-Dienst
#    (ollama), damit DocuSort spaeter auf einen Klick ganz ohne Cloud arbeiten
#    kann. Der belegt Platz — gemessen rund 5,5 GB, weil die
#    Grafikkarten-Bibliotheken mit drin sind —, und wer Platz belegt, sagt es
#    vorher. Das MODELL selbst (mehrere GB) wird NICHT jetzt geladen, sondern
#    erst, wenn jemand in DocuSort darauf drueckt.
say "Pulling images (DocuSort, plus a local AI service of about 5.5 GB so you"
say "can work without any cloud later — the model itself is only fetched when"
say "you ask for it in DocuSort). Not wanted? docker compose stop ollama"

say "Pulling $IMAGE"
# 🔴 Ein fehlgeschlagener Abruf ist nicht dasselbe wie ein fehlendes Abbild.
#    Mit `pull && up -d` riss eine kurze Netzstoerung (oder eine Registry, die
#    gerade nicht mag) die ganze Installation mit, obwohl das Abbild schon auf
#    der Maschine lag. Erst fragen, dann abbrechen.
if ! ( cd "$DIR" && $COMPOSE pull ); then
  if docker image inspect "$IMAGE" >/dev/null 2>&1; then
    warn "could not fetch $IMAGE — using the copy already on this machine."
  else
    die "could not fetch $IMAGE, and there is no copy on this machine."
  fi
fi
( cd "$DIR" && $COMPOSE up -d )

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

# ══════════════════════════════════════════════════════════════════════════
# 🔑 ZWEI ARCHIVE ZU EINEM (06.10.2026)
#
# Wer durch den alten Installer zwei Datenverzeichnisse hat, hat seine Post in
# zwei Haelften — und hat die zweite oft von Hand noch einmal hochgeladen. Das
# laesst sich zusammenlegen, und zwar ohne dass etwas doppelt wird: DocuSort
# erkennt dasselbe Dokument am INHALT (SHA256), Buchungen an ihrem Hash.
#
# 🔴 Gefragt wird ueber /dev/tty, nicht ueber stdin — bei `curl | bash` ist
#    stdin das SKRIPT. Ohne Terminal wird nichts getan und der Befehl gezeigt.
# 🔴 Und der Container wird dafuer angehalten: zwei Schreiber auf einer
#    SQLite-Datei sind ein Weg, beide Haelften zu verlieren.
# ══════════════════════════════════════════════════════════════════════════
if [ -n "${ZUSAMMEN:-}" ] && [ -f "$DIR/data/library/docusort.db" ]; then
  for _w in $ZUSAMMEN; do
    [ -f "$_w/data/library/docusort.db" ] || continue
    say ""
    say "Another archive is lying next to this one:"
    say "   $_w/data"
    say "Looking at what it would add (nothing is written):"
    if ! docker run --rm \
         -v "$DIR/data:/data" -v "$DIR/config:/app/config" \
         -v "$_w/data:/fremd:ro" "$IMAGE" \
         python -m docusort --merge-from /fremd 2>&1 | sed 's/^/     /'; then
      warn "   Could not look into it — leaving both alone."
      continue
    fi
    _JA=""
    if [ -r /dev/tty ]; then
      printf '\n  Merge it into this installation? Nothing is deleted, and the\n'
      printf '  database is backed up first. [y/N] '
      read -r _JA < /dev/tty || _JA=""
      printf '\n'
    fi
    case "$_JA" in
      [yYjJ]*)
        say "Stopping DocuSort for the merge …"
        ( cd "$DIR" && $COMPOSE stop docusort >/dev/null 2>&1 || true )
        if docker run --rm \
           -v "$DIR/data:/data" -v "$DIR/config:/app/config" \
           -v "$_w/data:/fremd:ro" "$IMAGE" \
           python -m docusort --merge-from /fremd --merge-apply \
           2>&1 | sed 's/^/     /'; then
          say "Merged. The other folder is untouched — delete it when you are sure."
        else
          warn "The merge did not finish. Both archives are as they were,"
          warn "and a backup of this one lies next to its database."
        fi
        ( cd "$DIR" && $COMPOSE start docusort >/dev/null 2>&1 || true )
        ;;
      *)
        say "Left alone. To do it later:"
        say "   docker run --rm -v $DIR/data:/data -v $DIR/config:/app/config \\"
        say "     -v $_w/data:/fremd:ro $IMAGE \\"
        say "     python -m docusort --merge-from /fremd --merge-apply"
        ;;
    esac
  done
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

  Updates:       they happen by themselves, once an hour — the compose
                 file this installer wrote carries a watchtower service that
                 watches only the docusort container. Measured, not promised:
                 `docker logs docusort-watchtower` prints its next run.
                 To switch it off, delete that service. To move the time, put
                 WATCHTOWER_SCHEDULE=0 0 4 * * * in .env (that one is
                 nightly at 04:00). An existing installation switches to
                 hourly the same way: WATCHTOWER_SCHEDULE=0 0 * * * *
  Update now:    cd $DIR && $COMPOSE pull && $COMPOSE up -d
  On the phone:  reach it from anywhere over Tailscale, with nothing exposed
                 to the internet and no port forwarding — one command:
                   cd $DIR && curl -fsSL \
                     https://raw.githubusercontent.com/robeertm/DocuSort/main/deploy/tailscale.sh \
                     | bash -s -- tskey-auth-…
  Logs:          cd $DIR && $COMPOSE logs -f

DONE
