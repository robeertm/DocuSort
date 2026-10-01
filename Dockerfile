FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Berlin \
    DOCUSORT_CONFIG_DIR=/app/config

# System dependencies for OCR: Tesseract + German/English language packs,
# ocrmypdf and its prerequisites, ghostscript for PDF handling.
# poppler-utils carries pdftoppm, which renders the page images for the
# document preview (iOS ignores PDF view parameters inside an iframe).
RUN apt-get update && apt-get install -y --no-install-recommends \
        poppler-utils \
        tesseract-ocr \
        tesseract-ocr-deu \
        tesseract-ocr-eng \
        ocrmypdf \
        ghostscript \
        qpdf \
        pngquant \
        unpaper \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

# ── Tailscale, mit im Abbild ────────────────────────────────────────────────
# 🔑 WARUM IM ABBILD UND NICHT ALS BEIWAGEN
# Ein Beiwagen heisst: eine compose-Datei aendern, auf der Kommandozeile, auf
# dem Rechner. Hier drin heisst: ein Feld und ein Knopf in den Einstellungen.
#
# 🔑 WARUM DAS OHNE SONDERRECHTE GEHT — gemessen, nicht gehofft:
# `tailscaled --tun=userspace-networking` braucht WEDER `NET_ADMIN` NOCH
# `/dev/net/tun`. In einem nackten Container gestartet meldet es sauber
# „Logged out." und wartet auf einen Schluessel. Genau deshalb ist dieser Weg
# ueberhaupt moeglich.
#
# `TARGETARCH` setzt Docker beim Bauen je Architektur (amd64 / arm64) — ohne
# das zoege ein Abbild die Binaerdateien der falschen Maschine.
ARG TARGETARCH
ARG TAILSCALE_VERSION=1.86.2
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends curl ca-certificates; \
    curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${TAILSCALE_VERSION}_${TARGETARCH}.tgz" \
      -o /tmp/ts.tgz; \
    tar xzf /tmp/ts.tgz -C /tmp; \
    mv /tmp/tailscale_${TAILSCALE_VERSION}_${TARGETARCH}/tailscaled /usr/local/bin/; \
    mv /tmp/tailscale_${TAILSCALE_VERSION}_${TARGETARCH}/tailscale  /usr/local/bin/; \
    rm -rf /tmp/ts.tgz /tmp/tailscale_*; \
    apt-get purge -y curl && apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/*; \
    tailscaled --version

WORKDIR /app

# Tells the in-app updater which world it is in. Inside the image the code
# is part of the image, so the file-swapping self-update must not run — the
# next restart would hand the old code back without a word.
ENV DOCUSORT_IN_DOCKER=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY docusort /app/docusort
COPY config /app/config-default

# On first start, copy default config to the (mounted) config directory
# if it's empty, so users get a working setup out of the box.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 9876

VOLUME ["/data", "/app/config", "/app/logs"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "-m", "docusort"]
