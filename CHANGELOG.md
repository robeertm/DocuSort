# Changelog

All notable changes to DocuSort will be documented in this file.

## [0.14.1] – 2026-04-28

### Fixed

- Replaced personal example names that had crept into code comments,
  changelog entries, prompt few-shot examples and the settings
  placeholder with the standard German placeholder names
  ("Max Mustermann" / "Erika Mustermann"). No functional change.

## [0.14.0] – 2026-04-28

### Added — eight new charts on `/finance`

- **KPI strip**: average daily spend, peak daily spend, biggest
  single transaction with counterparty, busiest month by booking
  count.
- **Activity heatmap**: GitHub-style calendar grid showing the last
  several months of daily spending intensity. Empty days get a
  neutral cell, busier days shade through emerald to bright green.
  Anchored to the latest booking date in the database, so the chart
  stays useful even when the most recent statement isn't from this
  week.
- **Categories over time**: stacked bar per month showing how spend
  splits across the top categories. Smaller categories collapse into
  an "Others" bucket with a count so the legend stays readable.
- **Spend by weekday**: which day of the week eats the most money.
  Bar per weekday with both total spend and booking count.
- **Spend by day of month**: 31 narrow bars revealing the typical
  rent / subscription cluster on the 1st and a smoother spread of
  card payments throughout the month.
- **Transaction type breakdown**: stacked horizontal bar plus a
  legend showing how spend distributes across direct debit, card
  payment, standing order, cash withdrawals, fees, etc.
- **Counterparty heatmap**: top counterparties as a tile heap sized
  proportionally to spend. Click a tile to filter the transaction
  table by that counterparty.
- **Balance trajectory**: SVG line chart per account showing the
  running balance over time, anchored to the earliest statement's
  opening balance and walked forward by every booking.
- **Largest single bookings**: top fifteen transactions by absolute
  amount, with counterparty, category and a short purpose snippet.

### Added — household-name privacy setting

- New `finance.holder_names` list in `config.yaml` and a textarea on
  `/settings` to manage it. Every name in the list is always masked
  before the OCR text reaches a cloud LLM, regardless of whether
  the document has a structured cue (Kontoinhaber: …, Herrn und
  Frau …) that the auto-detector would otherwise pick up. Lets the
  user reliably mask family members who only appear in body text
  of contracts and card-issuance letters.

### Fixed — pseudonymisation was missing names in body text

Investigated systematically across the user's full document set.
Findings and fixes:

- **Token decomposition**: when a full name like "Max Mustermann"
  was captured from the address block, the cross-text sweep
  replaced "Max Mustermann" everywhere — but a partner showing up
  only as "Erika Mustermann" in body text slipped through because
  her full name was never captured. The pseudonymiser now also
  registers each meaningful sub-token (the surname, the first
  names) as its own mask entry, so the family surname masks every
  family member's mention.
- **Letter-boundary sweep**: the previous word-boundary regex didn't
  fire between a digit and a letter, so booking lines like
  "MKTNR. 1220-1108-07MUSTERMANN" leaked the name. The sweep now
  uses negative letter look-arounds — digits, punctuation and
  whitespace all count as separators while still preventing
  mid-word substitutions.
- **Inline salutation pattern**: catches "Herrn Max Mustermann"
  written on a single line, common in card-issuance letters where
  the address block is one long flowing paragraph rather than a
  clean stack.
- **Inhaber-cue extraction with strict colon**: matches
  "Karteninhaber: …", "Darlehensnehmer: …", "Gutschriftskonto­
  inhaber: …" and similar formal-document cues. The previous
  loose version captured German legal text like "Hauptantragsteller
  zahlt das Darlehen" because IGNORECASE flipped its case-sensitive
  capital-letter-start safeguard.
- **Case-sensitive structured patterns**: dropped IGNORECASE from
  the salutation, holder-line and inhaber regexes so the
  capital-letter requirement on names actually filters out
  lowercase verbs.

Verified end-to-end across every Kontoauszug and Bank document on
the user's instance: zero occurrences of the family's real names
in any outbound payload.

## [0.13.6] – 2026-04-28

### Fixed

- **Statements with many bookings extract correctly.** The
  Kontoauszug extractor was capped at a small output budget that
  multi-page Privatgirokonto statements blew past — the AI would
  start emitting the booking list, get cut off mid-string, and the
  parser would fall back to a single inner booking object that
  carried no `transactions` field. Result: a statement that
  silently saved with empty bank, empty period, zero bookings.
  Output budget is now generous enough for a year of card
  payments, so the AI emits valid JSON to the end.
- **Long statement calls don't time out.** Generating that much
  output runs past the default per-call timeout. The provider now
  accepts a per-request timeout override and the statement
  extractor uses it; long extractions stay open instead of
  reporting a network timeout halfway through.

### Changed

- All providers gained an optional `timeout` parameter on `classify`
  for the same reason — short classification calls stay snappy,
  heavy extraction calls get the time they need.

## [0.13.5] – 2026-04-28

### Fixed

- **Bank statements no longer get truncated mid-table.** New uploads
  used to store only the leading section of a multi-page statement,
  enough for category classification but too short to capture the
  full booking table. The booking section now lands in the database
  intact, so the second-pass extractor for Kontoauszüge sees the
  whole document.

### Added

- **Refresh OCR for existing statements** without paying for a new
  AI pass: the new `docusort --reocr-statements` command (also
  exposed as `POST /api/finance/reocr-all`) walks every Kontoauszug
  / Bank document, re-reads the PDF, and overwrites the stored OCR
  text with the full content. Pure local work — fixes documents
  that were imported before the truncation change without a
  re-upload. After it runs, the existing "Erneut auswerten" button
  on `/finance` can finish what was previously stuck.

## [0.13.4] – 2026-04-28

### Changed

- Reverted the changelog back to English. The number-free style stays —
  entries describe what changes for the user, not which thresholds
  shifted under the hood.

## [0.13.3] – 2026-04-28

### Changed

- Changelog rewritten to drop the numeric clutter: token limits,
  retry counts, character thresholds and "N new strings × M
  languages" lines are gone. Entries now stick to what changes for
  the user.

## [0.13.2] – 2026-04-27

### Changed

- **Headline cashflow on `/finance` excludes internal transfers.**
  Money you move between your own accounts (closing a Tagesgeld and
  crediting the Giro, sweeping the Giro back to a savings account)
  no longer counts as "income" or "expense". Those bookings show up
  as a separate note on the transactions card and stay visible in
  the category breakdown — they just don't distort the chart any
  more.
- **Auto-detect internal transfers.** When a transaction's
  counterparty matches the account holder's name (including joint
  accounts where the partner's name appears on the booking line),
  the extractor tags it as a transfer at extract time. Catches the
  common Sparkasse pattern where internal moves print without an
  obvious "Übertrag" label.
- **Multi-page statements get read in full.** PDFs with long
  booking tables used to be truncated before the AI saw the second
  page. The cap is now generous enough that typical monthly and
  quarterly statements arrive intact.
- **Statement period no longer flips back to front.** When the AI
  swaps start and end dates — a common mistake because the
  document-issue date sits at the top while the booking range sits
  in the table — the values get reordered automatically. The
  prompt also tells the AI more clearly that the header date is
  not the booking period.

### Added

- **Diagnostics banner on `/finance`** that names every uploaded
  Kontoauszug whose extraction came back without transactions, with
  a one-click button to re-run the analysis on just that subset.
  Gaps stop hiding inside aggregate counts and become both visible
  and fixable.
- **Delete an account directly from `/finance`.** If the old IBAN
  fallback created a bogus "Unbekannt …xxxx" account (see below),
  it goes away with a click. The booked transactions stay in the
  database and re-attach to the right account on the next
  extraction.

### Fixed

- **No more ghost accounts from the IBAN fallback.** When the AI
  was unsure which IBAN belonged to the user's own account, earlier
  versions guessed the first IBAN seen in the text — frequently a
  counterparty IBAN — and every unsure statement piled up under a
  single fake account. The account now stays empty when the
  assignment isn't unambiguous; bookings can be reattached later.
- **Anthropic rate-limit recovers itself.** The "please wait"
  response from the API used to hard-fail the run. DocuSort now
  honours the wait time the server hands back and continues
  automatically, repeatedly if needed.
- **Anthropic spending caps are reported clearly.** Hitting a
  self-set spending limit on the Anthropic console used to look
  like a generic transient error. The message now points at the
  limit setting in the Anthropic console so it's obvious where to
  go.
- **Backfill paces itself.** When processing a stack of existing
  statements, DocuSort inserts small pauses between AI calls so
  the per-minute token budget doesn't burn down in seconds.

### Translations

- New strings for the diagnostics banner, the account-delete dialog
  and the internal-transfer note — in all five UI languages.

## [0.13.1] – 2026-04-27

### Added — Privacy transparency

- **"What gets sent to the AI?"** is now expandable on every
  Kontoauszug detail page. One click — computed locally, no AI
  call, no cost — shows what would actually be transmitted:
  privacy mode, provider and model, payload size, how many IBANs
  / names / addresses / emails were masked, the full pseudonymised
  text exactly as the AI sees it, and the local token-to-value
  reverse map that never leaves your server. The abstract privacy
  promise becomes something you can audit yourself.
- **Statement card on the document page**: bank, period, opening
  and closing balance, transaction list, privacy badge, manual
  extract / re-extract button. Surfaces both for explicit
  Kontoauszug documents and for legacy Bank/Konto documents that
  predate the new category.

### Fixed

- **Classifier picks Kontoauszug more reliably.** The Bank
  category description used to mention "Kontoauszüge" which made
  the AI dither between Bank/Konto and Kontoauszug — and pick
  Bank in practice. Bank now covers contracts, securities and
  loans only; current statements go cleanly into the new
  Kontoauszug category.
- **Backfill processes the legacy stash too.** Statements that
  ended up under Bank/Konto before the Kontoauszug category
  existed now get picked up by the bulk backfill and promoted to
  the right category after a successful extraction.
- **Sharper pseudonymisation.** The multi-line address block at
  the top of German bank statements ("Herrn und Frau / first name
  surname / street / postcode city") is now recognised and masked
  in full, and any further mentions of the same name inside
  booking lines get replaced in the same pass — including ALL
  CAPS, which banks like to use for booking texts. Anyone who saw
  "NAME_001" as the account holder because the AI invented a
  token without backing: that's filtered out now.

## [0.13.0] – 2026-04-27

### Added — Bank statement analysis (`/finance`)

- **New top-level category Kontoauszug** for Giro, Tagesgeld,
  credit-card, securities-account and PayPal statements. Picked up
  by the classifier automatically — no manual flagging needed.
- **Dedicated `/finance` tab** with a cashflow chart (income up in
  green, expenses down in red, shared scale), accounts list,
  category breakdown, top counterparties (incoming and outgoing),
  detection of recurring bookings (subscriptions, rent, insurance)
  and a search-and-filter transaction table.
- **Account and transaction extraction from the statement text.**
  A second AI pass pulls bank, period, opening and closing balance
  out of the OCR text together with every booking — date,
  signed amount, counterparty, IBAN, purpose, type and category.
  Multiple statements for the same account auto-merge through a
  hash of the IBAN, without ever revealing the real IBAN to the
  cloud provider.
- **Cross-statement deduplication.** When monthly and quarterly
  statements overlap, each booking is still counted only once.
  Identical PDFs don't import twice in the first place.
- **Pseudonymisation before every cloud AI call.** IBANs, email
  addresses, postal addresses and the account holder's name are
  swapped for stable tokens before the OCR text leaves the box.
  Counterparty names like Lidl or Stadtwerke stay visible — the AI
  needs them to categorise — and the tokens get translated back to
  real values locally once the response comes in.
- **Privacy controls under `/settings`**: choose between cloud AI
  with pseudonymisation (default) and a strict local-only mode.
  Local-only refuses to process statements unless a local provider
  is configured, so there's no accidental cloud transmission. The
  privacy mode of the most recently processed statement is shown
  as a badge on `/finance`.
- **`docusort --backfill-statements`** to extract bookings from
  existing Kontoauszug documents that don't have them yet. Uses
  the stored OCR text, so only the AI call costs anything, no
  re-OCR.

### Changed

- New "Finance" entry in the main navigation, also reachable from
  the mobile scroll bar.
- Privacy settings can be toggled live without restarting the
  service.

## [0.12.7] – 2026-04-27

### Added

- **Receipts are now editable.** Click any field on the receipt card to
  fix what OCR got wrong — shop name, type, payment method, date,
  every line item (name, qty, unit price, total, category), and the
  total amount. A live "sum of items" subtotal flags mismatches between
  the line items and the printed total.
- **`PATCH /api/document/{doc_id}/receipt`** accepts the full receipt
  (header + items) and replaces it atomically. Whitelists shop type,
  payment method, and item category so bad input fails loud.
- **Re-extract guard.** The "neu auswerten" button now confirms before
  overwriting manual edits.

### Changed

- **Tab renamed: "Auswertung" → "Kassenzettel"** in all five languages
  (Receipts / Tickets de caisse / Scontrini / Recibos). The route stays
  `/analytics` for now.

## [0.12.6] – 2026-04-26

### Added

- **Web server host + port editable from the UI.** New "Web-Server"
  section on the settings page lets you change `web.host` and
  `web.port` without touching `config.yaml`. The setup wizard's AI
  step gained an "Advanced (host & port)" disclosure so the values
  can be customised on first run too.
- **`POST /api/settings/web`** writes the new values to `config.yaml`,
  validates port range (1–65535), and returns `restart_required: true`
  so the UI can prompt for a service restart.

### Changed

- README no longer hardcodes port `8080` everywhere — uses
  `<port>` placeholder with "default 8080, configurable in /settings"
  notes. Privileged ports (<1024) get an explicit warning since
  DocuSort doesn't run as root.

## [0.12.5] – 2026-04-26

### Added

- **README screenshot gallery.** Six PNGs in `docs/screenshots/` —
  dashboard, library, analytics, settings, backup, upload — embedded
  near the top of the README so the project page is no longer
  text-only.
- **GitHub repository topics** for discoverability:
  `document-management`, `document-organizer`, `document-classifier`,
  `ocr`, `ai`, `llm`, `claude`, `openai`, `gemini`, `ollama`,
  `self-hosted`, `synology`, `nas`, `python`, `fastapi`, `htmx`,
  `receipt-scanner`, `expense-tracker`, `local-first`, `privacy`.
- Repo description rewritten and the typo (`docomentscanner`) fixed.

## [0.12.4] – 2026-04-26

### Changed

- **License changed from MIT to a proprietary "Use, don't modify" license.**
  Personal use and source inspection remain permitted; modification,
  redistribution, derivative works, and commercial use now require prior
  written permission. All rights reserved by Max Mustermann.
  Versions up to and including v0.12.3 remain available under the MIT
  License for anyone who already obtained a copy of those releases —
  this change is not retroactive.
- `__license__` in `docusort/__init__.py` updated from `MIT` to
  `Proprietary`.
- README license section rewritten accordingly.

## [0.12.3] – 2026-04-26

### Fixed

- **Folder picker modal got painted over by sibling cards.** The
  `.card` class uses `backdrop-blur`, which creates its own stacking
  context — that meant a `position: fixed z-40` modal nested inside
  one card couldn't paint over a *later* sibling card with its own
  stacking context. The "Speicherorte" card under the Backup section
  showed through the modal. Modal is now teleported to `<body>` via
  Alpine's `x-teleport`, escaping the card's stacking context entirely.
  Z-index bumped to 50 and backdrop opacity from `/60` to `/70` for a
  bit more contrast.

## [0.12.2] – 2026-04-26

### Fixed

- **Folder picker's "Diesen Ordner wählen" button could be cut off
  below the viewport.** The modal had no height cap, so on shorter
  windows the directory list pushed the action bar past the visible
  area. Modal is now `max-h-[calc(100vh-2rem)]` flex-column with a
  scrollable list region (`flex-1 min-h-0 overflow-y-auto`), so the
  Pick / Back buttons stay glued to the bottom of the modal at all
  viewport heights.

### Changed

- **Folder picker no longer opens at `/`.** Defaults to the user's home
  directory so the first thing visible is something the user owns,
  not the system root with `bin/`, `boot/`, `dev/`, `etc/`, `lib/`,
  `lost+found/` etc. — none of which are meaningful as backup targets.
- **Library parent dir added as a quick-jump shortcut** in the picker.
  Most users want their backup folder right next to the library, and
  navigating from `/` → `home` → `<user>` → `<library-parent>` for
  every fresh setup is friction.

## [0.12.1] – 2026-04-26

### Fixed

- **Mobile nav was missing Analytics and Settings.** Both links lived
  only in the desktop-only `hidden sm:inline-flex` row, so on phones
  the Analytics dashboard and Settings page were unreachable from the
  header. The bottom mobile nav now lists all five entries (Dashboard,
  Library, Review, Auswertung, Einstellungen) and is horizontally
  scrollable when the viewport can't fit them.

## [0.12.0] – 2026-04-26

### Added — Receipt scanner & analytics

- **Kassenzettel category** with shop-type subcategories (Supermarkt,
  Drogerie, Baumarkt, Restaurant, Cafe, Tankstelle, Apotheke,
  Bekleidung, Elektronik, Buecher, Moebel, Versand, Sonstiges). The
  classifier now recognises receipts and routes them via two new
  few-shot examples (Supermarkt, Tankquittung).
- **Second-pass line-item extractor** (`docusort/receipts.py`). When
  the main classifier marks a doc as Kassenzettel, a follow-up LLM
  call pulls out shop name + type, payment method, total, currency,
  date, and per-line `{name, quantity, unit_price, total_price,
  item_category}`. Item categories normalised to a fixed list
  (lebensmittel, getraenke, haushalt, koerperpflege, elektronik,
  bekleidung, buecher, essen-trinken-aussehaus, transport, baumarkt,
  tabak, pfand, rabatt, sonstiges). Discounts and Pfand rows are
  preserved with negative `total_price`.
- **`receipts` + `receipt_items` SQLite tables** with cascade-on-delete
  trigger. New `Database` methods: `upsert_receipt`, `get_receipt`,
  `delete_receipt`, `receipt_summary`, `receipt_monthly`,
  `receipts_list`, `receipt_items_search`, `top_items`.
- **Analytics dashboard at `/analytics`** — top-line stats (total spent,
  receipt count, item count, avg per receipt), 12-month spend chart,
  by-shop-type bar chart, by-item-category bar chart, top-15 most-bought
  items, recent receipts list, and an item search with shop-type +
  date-range filters. Empty-state with a CTA to upload the first receipt.
- **Receipt section on the document detail page**: shop badge, payment
  method badge, big total, item table (with negative-amount rows in
  rose). "Extract now" button when no receipt was extracted yet,
  "Re-extract" button to redo the LLM call.
- **`POST /api/document/{id}/receipt/extract`** for manual (re)extraction
  using the stored OCR text — no new OCR cost.
- **`GET /api/receipts/stats`** and **`GET /api/receipts/items`** so the
  data is consumable from outside the UI too (e.g. external dashboards).
- **`python -m docusort --backfill-receipts`** extracts line items from
  every existing Kassenzettel document that doesn't have a receipt yet.
  Reads cached OCR text — only the LLM call is billed.
- **Analytics nav link** in the header. **82 new i18n keys × 5 languages**
  for shop types, item categories, payment methods, analytics labels,
  and the receipt section.

## [0.11.3] – 2026-04-26

### Changed

- **Settings page now uses the full container width.** Previously
  capped at `max-w-3xl` (768px), so it looked narrower than the rest of
  the app. Now it uses the same `max-w-6xl` (1152px) the dashboard and
  library use, with cards stacked single-column inside.

### Fixed

- **CSS cache busting.** `<link rel="stylesheet" href="/static/tailwind.css">`
  now carries a `?v={{ version }}` query so a fresh build is picked up on
  the next page load instead of needing a hard reload.

## [0.11.2] – 2026-04-26

### Added

- **Folder picker modal** for the local backup path. Browse the
  server-side filesystem, jump to common roots (Home, /mnt, /media,
  /tmp, /data) and pick a folder visually instead of typing a path. Backed
  by a new `GET /api/fs/list?path=...` endpoint that lists directories,
  reports the parent for back-navigation, and gracefully falls back to
  the closest existing ancestor when the path doesn't exist.
- **Cloud-sync section reorganised by friction.** WebDAV / SFTP / S3 are
  now the primary tiles ("simple options — no OAuth needed"); Drive,
  Dropbox and OneDrive are folded behind a "Show OAuth providers"
  reveal with a yellow notice that they need a one-time token dance on
  a separate machine.

### Fixed

- **Legacy `ANTHROPIC_API_KEY` env var was not detected by /settings.**
  The page reported "no key stored" even when the env var was clearly
  set; the AI form rejected blank submissions for that reason. Both
  paths now consult `get_api_key()`, which checks `secrets.yaml` *and*
  falls back to the legacy env vars.

## [0.11.1] – 2026-04-26

### Added

- **Local-folder backup (rsync).** A new `target_type: local` mode for
  `sync.*` mirrors the library to any path on the host — a mounted USB
  stick, NAS share, NFS/SMB mount, second disk. No tokens, no OAuth,
  no rclone needed for this path. Uses `rsync -a --delete --delete-excluded`
  with `_Trash/` excluded; falls back to a pure-Python copy when rsync
  isn't on PATH.
- **`POST /api/sync/check-path`** quickly probes a candidate target:
  validates it isn't inside the library (would self-overwrite), checks
  writability with a touch+delete, and returns free disk space so the
  UI can show "Path ready · 78.2 GB free" while the user types.
- **Broken-remote detection.** `list_remotes()` now returns
  `{healthy, problem}` per remote — OAuth backends with an empty
  `token` field surface as `problem: "empty_token"`. The settings page
  badges them red and offers a one-click **Reconnect** button that
  opens the token-paste form pre-filled with the remote's name.
- **`POST /api/sync/run`** now dispatches between the rclone path and
  the local rsync path based on `sync.target_type`. Status surface
  includes the new fields (`target_type`, `local_path`, `rsync_installed`).

### Changed

- **Backup section UI redesigned.** Two big tiles up front: 📁
  "Local folder / NAS mount" (badged "Recommended") and ☁️
  "Cloud storage (rclone)" (badged "Advanced"). Setup wizard step 3
  follows the same pattern with local as the default.
- **`SyncSettings`** gained `target_type: str` and `local_path: str`,
  defaulting to `local` and `""` respectively. The legacy `remote`
  field is preserved for the rclone path.

## [0.11.0] – 2026-04-26

### Added

- **Multi-provider AI classifier.** A new `docusort/providers/`
  package abstracts the model call so DocuSort can talk to Anthropic
  Claude, OpenAI GPT, Google Gemini, or any OpenAI-Chat-Completions-
  compatible endpoint (Ollama, Groq, xAI, Mistral, Together,
  OpenRouter, …). Provider selection lives in `config.yaml` under
  `ai.provider` / `ai.model` / `ai.base_url`; the legacy `claude:`
  block is still honoured.
- **Local AI via Ollama.** Pick `provider: openai_compat` with
  `base_url: http://localhost:11434/v1` and a model name like
  `llama3.1` or `qwen2.5` — DocuSort talks to the local engine via the
  same OpenAI-compat path used for cloud providers. Cost is recorded
  as 0 for local models since there's no per-token charge.
- **Cross-provider pricing table** in `providers/pricing.py` covers
  Anthropic (with cache write/read multipliers), OpenAI (with cached
  prompt-token discount), and Gemini. `db.calculate_cost(model, …)`
  still works for legacy callers — it infers the provider from the
  model name and dispatches to the new table.
- **Setup wizard** at `/setup` — four-step flow (language → provider
  + token → backup → done) with provider cards, model defaults per
  provider, and a final "Restart & open" button that triggers the
  systemd restart hook.
- **Settings page** at `/settings`, always reachable from the header
  cog. Same fields as the wizard but as a single page; provider
  switch is non-destructive — each provider's API key keeps its own
  slot in `secrets.yaml`, so flipping back and forth doesn't lose
  anything.
- **First-run gate.** When no provider is configured, every request
  except `/setup`, `/static/*`, and a small allow-list of setup APIs
  redirects to the wizard. JSON callers get HTTP 503 with a clear
  message instead of an HTML page. The watcher logs and skips
  classification until the wizard is finished.
- **Headless rclone remote setup.** Instead of running `rclone config`
  on the headless box (which tries to open a browser and fails),
  DocuSort writes `rclone.conf` directly. For OAuth backends
  (Drive / Dropbox / OneDrive) the user runs `rclone authorize "drive"`
  on a laptop and pastes the resulting JSON token into a textarea.
  S3-compatibles, WebDAV, and SFTP get plain forms (access keys, URL,
  user/password). All managed via new endpoints under
  `/api/sync/remote/*` and `/api/sync/remotes`.
- **API key storage in `config/secrets.yaml`.** Written with mode 0600
  by `save_secrets()`, read back at runtime by `get_api_key()`. Also
  honours the legacy `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` /
  `GEMINI_API_KEY` env vars as a fallback.
- **`PyPI`: `openai>=1.50` and `google-genai>=0.3`** added to
  `requirements.txt` (lazy-imported, so users who only run Anthropic
  don't pay an import cost).

### Changed

- **`config.AppSettings.ai`** replaces `claude` (kept as a property
  alias). Same fields — `provider`, `model`, `base_url`,
  `max_text_chars`, `min_confidence`, `timeout_seconds`.
- **`Classifier.__init__`** takes `AISettings` and an optional
  `Provider` instance (built via `build_provider()` if omitted).
  The classify path is unchanged otherwise — same prompt, same
  validation, same return type.
- **`/api/pricing`** now flattens all provider tables into one map so
  the dashboard JS doesn't need to know about provider names.
- **i18n: 81 new keys × 5 languages** for wizard/settings/local-sync
  copy.

## [0.10.2] – 2026-04-26

### Fixed

- **"Year = —" tree bucket was unreachable.** Documents without a
  `doc_date` were grouped under a "—" year in the library tree, but
  clicking the link sent `?year=—` which the filter translated to
  `substr(doc_date, 1, 4) = '—'` — matched nothing. The bucket now
  carries a separate `key: "unknown"` field; URLs use that, and
  `list_documents()` translates `year="unknown"` to
  `(doc_date IS NULL OR doc_date = '')`.
- **Empty-trash button left the user staring at raw JSON.** The form
  POSTed to `/api/trash/empty`, which returns `{"purged": N}`, and the
  browser navigated to that response. Now done via fetch + reload.

## [0.10.1] – 2026-04-25

### Fixed

- **Backfill renamed unchanged files to `-N.pdf`.** `_uniquify` was being
  asked to dodge the file we were *moving*, so a re-tag that landed at
  the same path renamed `foo.pdf` to `foo-2.pdf` for no reason. Caught
  during the v0.10.0 dry-run on 60+ docs. `target_path()` now takes an
  optional `current_path` and short-circuits when the natural target
  equals it.
- **Dashboard upload-progress card stuck on "klassifizieren" forever.**
  IDB records never reached a terminal stage when the user closed the
  upload tab mid-batch — the dashboard widget read IDB but never asked
  the server. Reconciler now polls `/api/status/<inbox_name>` for each
  non-terminal IDB row on every tick (every 4 s) and patches the row
  back to the server's truth.

## [0.10.0] – 2026-04-25

### Added

- **Subcategories.** Each top-level category can declare a fixed list of
  subcategories in `categories.yaml`. Files are filed at
  `library/<year>/<category>/<subcategory>/` when present, otherwise as
  before. Five new top-level categories: `Auto`, `Bildung`, `Familie`,
  `Reise`, `Hobby`.
- **Tags.** Each document carries 0–8 free-form lowercase labels stored
  as a JSON array on the row. Surfaced as `#chips` on cards and the
  document detail page; click a chip to filter the library by that tag.
  Examples Claude reaches for: `police`, `mahnung`, `kuendigung`,
  `bescheid`, `quittung`, `vertrag`, `aenderung`, `nachweis`.
- **Backfill** for existing inventory: `python -m docusort --backfill-tags`
  re-asks Claude (using cached OCR text — no new OCR cost) to attach
  subcategory + tags to every doc that's missing them, renames the file
  via the existing template, and moves it into the matching subfolder.
  `--backfill-dry-run` prints what would change without touching disk.

### Changed

- The classifier now returns `subcategory` and `tags` alongside the
  existing fields. The system prompt grew by ~1 kB; cache layout is
  preserved so cache-hit rate stays unchanged for warm sessions.
- Document edit form gains a **Subcategory** dropdown (filtered to the
  selected category's whitelist) and a **Tags** comma-separated input.
- `POST /document/{id}/edit` accepts `subcategory` (validated against the
  parent's whitelist) and `tags` (cleaned + deduped + truncated to 8).
- File path moves now mirror the new structure: an edit that changes
  category, subcategory, or date will move the file to the right
  `<year>/<category>/<sub>/` directory automatically.
- `GET /library` accepts `?subcategory=...&tag=...` filters.

### Migration

- `documents.subcategory TEXT DEFAULT ''` and
  `documents.tags TEXT DEFAULT '[]'` columns are added on first start.
- Existing rows keep their category and just get empty subcategory/tags
  until you run `--backfill-tags`.

## [0.9.3] – 2026-04-25

### Added

- **Full metadata editing for any document.** The "Kategorie ändern"
  card on the document detail page is replaced by a four-field edit
  form: category, date, sender, subject. On save the file is renamed
  using the existing template and moved to `library/<year>/<category>/`
  matching the new metadata, with collision-safe uniquification. Useful
  for fixing the "review" pile that Claude wasn't sure about, but works
  on every doc.

### Changed

- `POST /document/{id}/recategorize` is gone, replaced by
  `POST /document/{id}/edit` accepting `category`, `doc_date`, `sender`,
  `subject` form fields. Internal route — no external callers.

## [0.9.2] – 2026-04-25

### Fixed

- **Web UI unreachable for minutes after a restart with a non-empty inbox.**
  `process_existing()` was running synchronously before `_start_web()`, so a
  crash that left N files behind would block port 9876 for `N × ~30–90 s`
  until the queue drained. The watcher and uvicorn now come up immediately;
  the inbox is drained from a daemon thread alongside.
- **OOM under upload bursts.** Multiple `ocrmypdf` processes plus Claude
  calls running fully in parallel could exhaust RAM on small VMs (the
  trigger for the 0.9.1→0.9.2 incident: a 57 MB PDF on top of an already
  busy pipeline → kernel OOM-kill → systemd restart loop). Pipeline now
  gates concurrent OCR+Claude work with a `BoundedSemaphore`.

### Added

- `ocr.max_parallel` in `config.yaml` (default `2`). Cap on concurrent
  OCR+Claude jobs. Raise it on beefy hardware, drop to `1` on tiny VMs.

## [0.9.1] – 2026-04-25

### Fixed

- Upload-page hint no longer mentions HTTPS as a path to background
  uploads. The Service-Worker path was removed in v0.9.0 to dodge
  Safari's IndexedDB-blob bug, so the previous text was misleading even
  on a properly TLS-secured deployment. New copy explains what *does*
  happen: keep the tab open until the bar finishes; already-uploaded
  documents keep being classified on the server even after closing.

## [0.9.0] – 2026-04-25

### Fixed

- **Upload corrupted to 0 bytes on Safari + HTTPS.** WebKit has a long-
  standing bug where `Blob`s round-tripped through IndexedDB (e.g. read
  by a Service Worker) come back empty. Reworked the upload pipeline to
  keep `File` references in a JS `Map` keyed by IDB id; IndexedDB now
  stores only metadata. Pending items left over from a tab close are
  marked `needs-repick` instead of silently uploading 0-byte files.
- The legacy `/upload-sw.js` worker is unregistered on page load so any
  cached SW from v0.7.x stops draining stale state.

### Added

- **Multiselect in the library**. Each card has a checkbox in the upper
  left; clicking it toggles the card into a global selection. A sticky
  bottom bar appears with bulk actions:
  - **In den Papierkorb** (or restore / purge in trash view)
  - **Kategorie ändern** dropdown — moves all selected files to the new
    category folder + updates DB rows
  - **ZIP der Auswahl** — streams a zip of just the selected docs
- New endpoints:
  - `POST /api/bulk/delete`
  - `POST /api/bulk/restore`
  - `POST /api/bulk/purge`
  - `POST /api/bulk/recategorize`  body `{ids: [...], category: "X"}`
  - `GET /api/export.zip?ids=1,2,3`

### Notes

- Tab-close mid-upload now loses only the in-flight files; previously
  uploaded items in the same batch are safely on the server. The user
  is told which items still need to be re-picked.

## [0.8.0] – 2026-04-24

### Added

- **Native HTTPS** via `web.ssl_cert` / `web.ssl_key` in config.yaml.
  When both files exist uvicorn is booted in TLS mode on the same port;
  no reverse proxy required. Without the keys the server still speaks
  plain HTTP as before.
- **`scripts/setup-tailscale-https.sh`** — one-shot installer that:
  1. Pulls a Let's Encrypt cert via `tailscale cert` for the host's
     MagicDNS name (e.g. `foo.tailnet.ts.net`).
  2. Places the PEM files in `/etc/docusort/certs/` with the right
     ownership / permissions.
  3. Installs a weekly systemd timer (`docusort-cert-renew.timer`) that
     renews via `tailscale cert` and restarts the service.
  4. Patches `config.yaml` with the `ssl_cert` / `ssl_key` paths.

### Notes

- Once HTTPS is up, the upload page's service worker registers
  successfully and background uploads (tab-close-safe) work.
- Cert renewal is a no-op until the existing cert is within 30 days of
  expiry — it's safe to run weekly.

## [0.7.2] – 2026-04-24

### Fixed

- **Upload queue stuck at 0 over plain HTTP.** Browsers only register
  service workers in a secure context (HTTPS or localhost), so on a
  plain-HTTP deployment like `http://<nas>:9876` the SW registration
  silently failed and `navigator.serviceWorker.ready` never resolved
  — all uploads sat in IndexedDB forever.
  - Added `window.isSecureContext` guard + a 3-second timeout around
    the registration wait.
  - When the SW is unavailable the page drains the IDB queue itself
    via `fetch`, still writing progress back to IDB so the dashboard
    widget sees live updates. The "tab can be closed" claim turns
    into "keep this tab open" so the user knows the difference.
- The `drain` entry point picks between SW and in-page automatically;
  no more hanging on `serviceWorker.ready`.

### Notes

- To get TRUE background uploads, expose the app over HTTPS. On a
  Tailscale-attached host that's one command — see README.

## [0.7.1] – 2026-04-24

### Added

- Dashboard shows a **live upload-progress card** while any file from
  the Service-Worker queue is still uploading or being classified. The
  card reads directly from IndexedDB (same store the service worker
  writes) and subscribes to `BroadcastChannel('docusort-upload')`, so
  it updates without polling the server. Click-through link jumps to
  `/upload` for per-file detail.

## [0.7.0] – 2026-04-24

### Added

- **True background uploads via Service Worker + IndexedDB.** Pick 50
  files, close the tab, they still upload and get classified. Files are
  stashed as Blobs in IndexedDB under `docusort-uploads`; a Service
  Worker (served from `/upload-sw.js` with root scope) drains the queue
  against `POST /upload` with 4 concurrent requests. The browser keeps
  the worker alive for ~30–120 s after the last tab closes, which is
  plenty for a few dozen PDFs over a LAN/Tailscale link.
- If the worker is killed mid-drain (browser reclaim, OS sleep, or
  longer inactivity), the next page load re-sends any item that still
  has a file blob but no `inbox_name`. Server-side SHA256 dedup catches
  stray double-uploads cleanly — no manual retry needed.
- `BroadcastChannel('docusort-upload')` pushes live stage updates from
  the worker to every open tab.
- Banner "Upload läuft im Hintergrund — du kannst den Tab schließen"
  appears while any item is active.

### Removed

- The "Upload abgebrochen — Tab geschlossen" error state, along with
  the localStorage queue it was built on. Items that were uploading at
  tab-close now just resume on next visit.

### Notes

- Chrome / Edge / Firefox / Safari 16+ all run Service Workers.
- iOS Safari kills the SW aggressively when a tab is backgrounded —
  in practice the first 20–30 seconds of background work still land,
  which covers a typical handful of scans but not a full 100-file batch.

## [0.6.3] – 2026-04-24

### Added

- **Persistent upload queue** — the per-file status list now survives tab
  close / reload / language switch. The queue is stored in `localStorage`;
  on page load, any file the server still owns (has an `inbox_name`) is
  picked back up by `/api/status/{inbox_name}` polling and you see the
  stage continue from wherever it was.
- Files that were uploading when the tab was closed are shown as "Upload
  abgebrochen" so you know to re-send them — the server never finished
  writing those bytes.
- **Verlauf leeren** button in the queue header keeps active items and
  drops everything already in a terminal stage.

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
