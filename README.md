# DocuSort

AI-powered document organizer with a **web UI**. Upload a scan from your
desktop or phone and – a few seconds later – it is renamed, dated, filed into
the right category, and browsable in a clean, mobile-friendly interface.

Built for a Synology NAS in Docker, but runs anywhere Docker runs.

- **Web UI** on port 8080 — dashboard, library browser with full-text search,
  per-document detail + PDF preview, mobile upload with camera capture
- **OCR** for scanned PDFs and images (Tesseract `deu+eng`)
- **Claude** (Anthropic API) classifies each document and extracts metadata
- **Automatic filing** into `Library/YYYY/Category/` with a clean filename
- **Cost tracking** per document + aggregated (tokens in/out, USD and EUR preview)
- **Low-confidence review folder** instead of wrong guesses, recategorize with one click
- **Safety copy** of every original kept in `_Processed/`

## File naming

Every filed document follows the same pattern:

```
YYYY-MM-DD_Category_Sender_Subject.pdf
```

Examples:

```
2026-02-14_Rechnungen_Vodafone_Mobilfunk-Februar.pdf
2026-01-03_Gesundheit_Hausarzt-Dr-Mueller_Blutbild.pdf
2026-03-20_Steuer_Finanzamt-Dresden_Bescheid-2024.pdf
```

The template is configurable in `config/config.yaml`.

## Folder layout

```
/data/
├── inbox/                    ← drop scans here
└── library/
    ├── 2026/
    │   ├── Rechnungen/
    │   ├── Vertraege/
    │   ├── Behoerde/
    │   ├── Gesundheit/
    │   ├── Gehalt/
    │   ├── Steuer/
    │   ├── Haus/
    │   ├── Versicherung/
    │   ├── Bank/
    │   └── Sonstiges/
    ├── _Review/              ← uncertain docs land here for manual sorting
    └── _Processed/           ← copy of every original file
```

## Requirements

- Docker and docker-compose (Synology: install "Container Manager" from Package
  Center, DSM 7.2+)
- An Anthropic API key with a billing profile attached
- A folder on your NAS where scans arrive (e.g. `/volume1/Scan`)
- A folder that will become your library (e.g. `/volume1/Dokumente`)

## Quick start on Synology

1. **Copy the project** to your NAS, e.g. to
   `/volume1/docker/docusort/`. Via File Station, SFTP, or:
   ```bash
   scp -r docusort admin@synology:/volume1/docker/
   ```

2. **Create the `.env` file** in the project folder:
   ```bash
   cd /volume1/docker/docusort
   cp .env.example .env
   nano .env     # fill in ANTHROPIC_API_KEY
   ```

3. **Adjust `docker-compose.yml`** if your paths differ. Defaults:
   ```yaml
   volumes:
     - /volume1/Scan:/data/inbox
     - /volume1/Dokumente:/data/library
     - /volume1/docker/docusort/config:/app/config
     - /volume1/docker/docusort/logs:/app/logs
   ```

4. **Build and start**:
   ```bash
   sudo docker compose up -d --build
   ```

5. **Check the logs**:
   ```bash
   sudo docker logs -f docusort
   ```

6. **Open the UI** at `http://<nas-ip>:8080` from any device on your network
   (or over Tailscale / VPN) — dashboard, upload, library and cost overview
   live there. Dropping a PDF into `/volume1/Scan` still works — it appears
   correctly named under `/volume1/Dokumente/2026/…/`.

## Quick start locally (Mac / Linux)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
export DOCUSORT_CONFIG_DIR=$PWD/config
# adjust paths in config/config.yaml, then:
python -m docusort
```

OCR needs system-level Tesseract and ocrmypdf installed
(`brew install tesseract tesseract-lang ocrmypdf` on macOS).

## Configuration

All behaviour is controlled by two YAML files in `config/`:

- `config.yaml` – paths, OCR settings, Claude model, thresholds
- `categories.yaml` – the list of categories and their Claude hints

Relevant knobs:

| Setting | Default | What it does |
|---|---|---|
| `claude.model` | `claude-haiku-4-5-20251001` | Fast & cheap. Use `claude-sonnet-4-6` for tougher documents. |
| `claude.min_confidence` | `0.65` | Documents below this go to `_Review` |
| `ocr.languages` | `deu+eng` | Tesseract language packs |
| `keep_original` | `true` | Keep an untouched copy of each original in `_Processed` |
| `dry_run` | `false` | Classify and log but don't move anything |

After changing config, restart the container:
```bash
sudo docker compose restart docusort
```

## CLI flags

```bash
python -m docusort            # watcher + web UI on :8080 (default in Docker)
python -m docusort --once     # process existing files and exit
python -m docusort --no-web   # watcher only, no UI
python -m docusort --dry-run  # classify + log, no moves
python -m docusort --version
```

## How it decides

1. File appears in `inbox/`.
2. Watcher waits until the file size stops changing (default 5 s).
3. If the PDF has no text layer, `ocrmypdf` adds one.
4. The first ~12 k characters go to Claude, together with the category list.
5. Claude replies with strict JSON: `category, date, sender, subject, confidence`.
6. Confidence ≥ 0.65 → move to `library/YYYY/Category/`.
   Lower → move to `_Review/` for a human look.
7. The original is copied to `_Processed/` before being removed from `inbox/`.

## Cost

Haiku 4.5 classifies a typical one-page letter for a fraction of a cent.
A batch of 1 000 documents per month usually stays well under EUR 1 in API fees.
Scale model up to Sonnet 4.6 only if you see classification errors – it is
~10× more expensive.

## Roadmap

- ~~Etappe 2: Web UI, cost tracking, SQLite + FTS5 search~~ — shipped in **v0.2.0**
- Etappe 3: Telegram / email notification on new file or `_Review` entry
- Etappe 4: Duplicate detection across the whole library
- Etappe 5: Automatic reminders for contract termination dates
- Etappe 6: Prompt caching for bulk imports (reuse system prompt across calls)

## License

MIT – see `LICENSE`.
