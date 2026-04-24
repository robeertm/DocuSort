FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Berlin \
    DOCUSORT_CONFIG_DIR=/app/config

# System dependencies for OCR: Tesseract + German/English language packs,
# ocrmypdf and its prerequisites, ghostscript for PDF handling.
RUN apt-get update && apt-get install -y --no-install-recommends \
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

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY docusort /app/docusort
COPY config /app/config-default

# On first start, copy default config to the (mounted) config directory
# if it's empty, so users get a working setup out of the box.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 8080

VOLUME ["/data", "/app/config", "/app/logs"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "-m", "docusort"]
