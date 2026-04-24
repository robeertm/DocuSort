# Changelog

All notable changes to DocuSort will be documented in this file.

## [0.6.2] – 2026-04-24

### Changed

- **Mobile PDF preview**: the iframe now renders at 40vh on phones (55vh
  on tablets, 75vh on desktop) — iOS Safari's PDF-in-iframe rendering
  was fighting the page, and the old 75vh filled the whole screen.
- Tap on the preview (mobile) or click the **Vollbild**-button (any
  size) opens the PDF in a new browser tab where the native PDF viewer
  handles full-screen / pinch-zoom / etc. properly.
- Image documents get a similar "click to open full size" affordance.

## [0.6.1] – 2026-04-24

### Fixed

- Dashboard "Recently processed" is now sorted by *processing* time
  (`created_at`) rather than document date — a scan of a 2024 letter
  taken today no longer gets buried below a 2026 invoice processed last
  week. The library card grid keeps the doc-date sort (it's used for
  archival browsing, not recency). Added an `order_by` parameter to
  `Database.list_documents()` to make this explicit.

## [0.6.0] – 2026-04-24

### Added

- **Trash** — every document gets a "Move to trash" button in the detail
  view. Trashed docs are moved into a `_Trash/` tree that mirrors the
  category layout, hidden from dashboard / library / tree / stats, and
  excluded from the content-hash dedup lookup. The tree sidebar shows a
  "Papierkorb" entry when trash is non-empty; from there individual docs
  can be restored or permanently purged, or the whole trash emptied.
- **ZIP export** — streaming download of the library (or a filtered
  subset: `?category=X&year=Y`). `_Trash/` excluded by default.
  Duplicate rows sharing a library_path are written once.
- **Cloud sync via rclone** — `sync:` section in `config.yaml` with
  `remote`, `source`, `extra_flags`, `timeout_seconds`. Works with any
  rclone backend (iCloud Drive, Google Drive, Dropbox, OneDrive,
  Synology C2, S3, WebDAV, SFTP, …). Dashboard shows install/config
  status, last-run timestamp, transferred bytes/files, and has a
  "Sync now" button that kicks off an async sync and polls status.
- DB: `deleted_at` column + `idx_documents_deleted` index (migrated
  idempotently on startup).
- Routes: `POST /api/document/{id}/delete|restore|purge`,
  `POST /api/trash/empty`, `GET /api/export.zip`,
  `GET /api/sync/status`, `POST /api/sync/run`.

### Changed

- `stats()`, `tree()`, `distinct_years()`, `find_by_hash()` all now
  exclude soft-deleted rows by default.
- `list_documents()` gains a `trash=True` parameter.

## [0.5.0] – 2026-04-24

### Added

- **Internationalisation**: 5 languages (German, English, French, Spanish,
  Italian). Translations live in `docusort/locales/*.json`. The UI picks
  the language from the `lang` cookie, then the browser's `Accept-Language`
  header, then the `web.default_language` option in `config.yaml`.
- **Language switcher** in the nav bar. Changes take effect immediately
  (sets the `lang` cookie, reloads the page).
- **Folder upload**: `webkitdirectory` picker lets you select a whole
  folder tree. All supported PDFs and images are enqueued, uploaded with
  a 4-concurrent cap, and classified — subfolders included. `.DS_Store`
  and other non-document files are skipped silently.
- **Retry for failed / review documents**: a "Retry" card on the document
  detail page re-sends the stored OCR text to Claude and refiles the
  document. No extra OCR cost. Token and $ usage accumulates on the row.
- **Robust JSON parsing** in the classifier: switched from a greedy regex
  to `json.JSONDecoder.raw_decode`, which stops at the end of the first
  complete JSON object. Fixes "Extra data: line 8 column 4" failures when
  the model adds commentary after its JSON.

### Changed

- Upload UI: the progress bar now shows only while a file is actually
  uploading. Once the server has the bytes, the per-file row switches to
  a live stage label (queued → processing → filed/review/duplicate).
  Fixes the "100 % bar sits there forever" feedback.
- Concurrent uploads are capped at 4 in the browser so the inbox doesn't
  flood the watcher with a thousand parallel writes from a folder import.
- `create_app()` accepts an optional `classifier` so the retry endpoint
  can reuse the live classifier instance from the watcher process.

## [0.4.0] – 2026-04-24

### Added

- **Self-updater** (`docusort/updater.py`). Asks the GitHub API for the
  latest release, downloads the tarball, and swaps in the new code
  atomically while preserving `.env`, `config/`, `.venv` and `logs/`.
- **UI banner** on every page — if a newer release is out, a button
  installs it and schedules a systemd restart. Response shows whether
  the restart succeeded or manual `sudo systemctl restart docusort` is
  needed. Powered by `/api/version` and `/api/update`.
- **CLI flags**: `docusort --check-update` and `docusort --update`. Exit
  codes make them scriptable (0 = no update, 1 = update available or
  other end, 2 = error).
- **Cross-platform launchers** for the project root:
  - `start.sh` — Linux and macOS, creates venv on first run, installs
    dependencies, warns about missing tesseract / ocrmypdf, refuses
    to start without `config/config.yaml` and `.env`.
  - `start.command` — double-clickable wrapper for macOS Finder.
  - `start.bat` — Windows equivalent including `.env` parsing.
- **Passwordless sudo helper** `scripts/install-sudoers-rule.sh` —
  installs a narrowly scoped rule that lets the current user run
  `systemctl restart docusort` without a password, which is what the
  in-UI updater needs to finish the job.

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
