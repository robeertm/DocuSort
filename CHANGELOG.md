# Changelog

All notable changes to DocuSort will be documented in this file.

## [0.3.0] – 2026-04-24

### Added

- **Prompt caching** for Claude. System prompt (category guide + 12 few-shot
  examples + processing rules) is now ~5k tokens and marked `cache_control:
  ephemeral`. First call pays a 1.25× surcharge to write the cache, every
  subsequent call within 5 minutes reads at 0.1×. On a typical bulk import of
  1000 documents this saves **60–70 %** on input tokens.
- **SHA256 duplicate detection** before OCR and Claude. Identical files are
  filed as `status='duplicate'` with the same metadata as the original — no
  OCR, no API call, no cost.
- **Live upload status** — the upload page now polls `/api/status/{name}`
  after each upload and shows per-file stage: `Hochladen → Warteschlange →
  OCR läuft → Klassifiziert als X / Review / Duplikat`, with a direct link
  to the filed document.
- **Dashboard savings strip**: tokens saved by cache + dollars saved by
  duplicate detection.
- **Document detail**: Cache-write / cache-read token counts and the
  document's SHA256.

### Fixed

- Nav-button layout on the dashboard. Tailwind Play CDN's `@apply` did not
  resolve nested `btn` references, so the upload icon stacked above its
  label. Utility classes are now emitted directly.

### Changed

- `Classifier` now returns `cache_creation_tokens` and `cache_read_tokens`.
- `calculate_cost()` accepts `cache_write` / `cache_read` and applies the
  1.25× / 0.10× multipliers.
- DB: new columns `content_hash`, `cache_creation_tokens`, `cache_read_tokens`;
  auto-migration on startup for existing databases.
- Classifier system prompt moved from brief rules to a full guide (10
  categories with signals & examples, 12 few-shot examples, common pitfalls,
  OCR-aware processing notes). Improves classification accuracy and is
  necessary to cross Haiku 4.5's cache-size minimum (~4k tokens).

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
