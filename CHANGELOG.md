# Changelog

All notable changes to DocuSort will be documented in this file.

## [0.2.1] – 2026-04-24

### Added

- **Treeview sidebar** in the library: year → category hierarchy with
  per-node document counts. Click a year to filter, click a category under
  it to drill down. Status quick-filters (Review / Fehler) appear below
  the tree when relevant.
- Active-filter breadcrumb chips above the grid — click the ✕ on a chip
  to drop that facet without losing the others.
- Alpine.js `@alpinejs/collapse` plugin for smooth expand/collapse.

## [0.2.0] – 2026-04-24

### Etappe 2 – Web UI, Cost-Tracking, Volltextsuche

The default container now runs both the watcher and a web UI on port 8080.
Open `http://<nas-ip>:8080` from your desktop or phone — no auth (pair with
Tailscale or another VPN for remote access).

### Added

- **SQLite database** alongside the library (`docusort.db`) storing per-document
  metadata, token usage and cost (USD + EUR preview).
- **Web UI** built with FastAPI + Jinja + HTMX + Tailwind CSS — no build step,
  runs in the same container. Dark mode, mobile-first.
  - Dashboard: totals, cost breakdown, category distribution, 12-month activity,
    recent documents.
  - Library: filter by category / year / status, live full-text search via
    HTMX (SQLite FTS5 over filename, sender, subject, reasoning, OCR text).
  - Document detail: embedded PDF preview, full metadata, per-document cost,
    Claude's reasoning, one-click recategorize (file is physically moved).
  - Upload: drag & drop or multi-file picker, mobile camera capture for
    direct phone-scan uploads, live progress bars.
- Price table for Haiku 4.5 / Sonnet 4.6 / Opus 4.7 with automatic cost
  calculation per document.
- `--no-web` flag for running just the watcher (legacy behaviour).
- FastAPI, uvicorn, jinja2 and python-multipart added to requirements.

### Changed

- `Classifier` now returns token usage, calculated cost and model name on each
  `Classification`.
- `extract_text()` returns an `OcrResult` with text, output path, OCR flag and
  page count (previously just `(text, path)`).
- Docker image now exposes port 8080; docker-compose maps it to the host.

## [0.1.1] – 2026-04-24

### Fixed

- Add missing `docusort/__main__.py` so `python -m docusort` works. Without
  this, the Docker container failed to start with
  `'docusort' is a package and cannot be directly executed`.

## [0.1.0] – 2026-04-24

### Etappe 1 – MVP

Initial release. Core pipeline complete and runnable in Docker on Synology DSM 7.2.

- Folder watcher (`watchdog`) with stable-size debouncing
- OCR via `ocrmypdf` (Tesseract `deu+eng`) for scanned PDFs, `pytesseract` for images
- Claude-powered classifier with strict JSON output
- 10 categories: Rechnungen, Vertraege, Behoerde, Gesundheit, Gehalt, Steuer,
  Haus, Versicherung, Bank, Sonstiges
- Year/category folder layout: `Dokumente/YYYY/Kategorie/`
- Filename template: `YYYY-MM-DD_Kategorie_Sender_Subject.pdf`
- Low-confidence routing to `_Review`, originals kept in `_Processed`
- Rotating log file + stdout
- One-shot (`--once`) and dry-run (`--dry-run`) modes
- Docker image with all system deps, docker-compose for Synology
