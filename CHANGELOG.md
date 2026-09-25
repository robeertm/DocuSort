# Changelog

All notable changes to DocuSort are documented here.
Dates are ISO; versions follow `MAJOR.MINOR.PATCH`.

This file starts with the first public release. The project was developed
privately before that; the summary under *0.1.0 – 0.55.0* lists what arrived
along the way rather than every single step.

## [0.59.0] – 2026-09-25

### Added
- **"Match against the finances" on the *Due soon* card.** The pass reads
  the invoice amount out of each document's own text and looks for the
  booking that settled it. It used to run at start-up, after an import and
  every twelve hours — now it also runs on demand, right where the word
  *overdue* is printed. The result is stated ("27 bill(s) linked to a
  booking") and the card reloads. New route
  `POST /api/finance/match-deadlines`.
- **A freshly filed document asks by itself.** At the end of the pipeline
  the pass is scheduled and runs about twenty seconds later.

### Changed
- 🔴 **A bill could wait up to twelve hours for its booking.** The amount
  is not a field in the document, it is *read* out of the text — and it was
  read on a twelve-hour cycle only. Without an amount there is nothing to
  compare, so a bill dropped in at noon sat on the dashboard as **overdue**
  until midnight although the direct debit had gone out long before.
- **All four callers now use one place** (new module `deadline_match`):
  start-up, the twelve-hour reminder watchdog, the button, and the
  pipeline. 🔴 Two passes must never overlap —
  `finance_match_due_payments` reads the set of already-linked bookings
  **once** at the start, so two concurrent runs could have handed the same
  booking to two different bills. A process-wide lock rules that out.
- **A burst costs one pass.** When forty-five invoices arrive at once the
  timer is restarted per document and the pass runs **once** after the last
  one — with a two-minute ceiling, so a steady trickle (one document every
  few seconds) cannot postpone it for ever.
- The CSV import now reads the amounts before matching. A bill whose amount
  had never been read could not be linked even when the matching booking
  had just been imported.
- A user, not only an admin, may trigger the pass. They can already link a
  booking to a document by hand (`POST /api/document/<id>/paid`); the pass
  asks the same question for every open bill at once.

## [0.58.1] – 2026-09-21

### Fixed
- 🔴 **The PDF preview did not fit its window on a phone.** It was an
  embedded viewer (`iframe`), and **iOS Safari ignores the fit-to-window
  parameter there**, showing the page at its original size — you saw the
  top left corner and nothing else. On a phone the preview is now a
  **page image rendered on the server** (Poppler / `pdftoppm`) that always
  fits the width; multi-page documents unfold below each other. The
  embedded viewer stays on the desktop and steps back in whenever the
  rendering is not possible (no Poppler, an encrypted file).
- Page images live next to the database in `preview-cache/` and carry the
  source file's modification time in their name, so the preview never
  shows a stale page after a re-run of OCR. Backups skip the folder.
- `poppler-utils` is now installed explicitly in the Docker image.

## [0.58.0] – 2026-09-21

### Changed
- **The phone view was rebuilt, not patched.** Until now the desktop
  layout was simply squeezed: ten table columns on 386 pixels, controls
  26 pixels tall, nearly a thousand pieces of text below 12.5 pixels. The
  phone now has a shape of its own.
- **A tab bar at the bottom** replaces the scrolling strip under the
  header: Home, Library, Upload (in the middle, because everything comes
  in there), Finance and “More”. Behind *More* sits a sheet with every
  other section plus language, appearance and sign-out. The old strip
  showed three entries; the rest hid behind a swipe nobody expects.
- **Tables become lists.** Fixed costs, bookings, receipts, line items
  and the recent bookings on the finance page each show one row per
  entry: name and amount on top, the essentials below, the category
  across the full width. From tablet width upwards the tables are
  unchanged.
- **Everything you tap is at least 44 pixels tall** (Apple's guideline)
  and input fields carry 16-pixel text — below that iOS zooms in on
  focus, unasked.
- **Readable type**: the smallest steps are one notch larger on a phone.
  Chart axes are excluded, where the text decides the column width.
- **Filters fold away.** Bookings put a two-screen filter card in front
  of the first booking; the library put 1,300 pixels of filters in front
  of the first document. Both now open on tap.
- **Bookings load forty at a time**: the page was 16,000 pixels long, now
  8,000 with a button for more.
- The daily chart is taller on a phone and starts at the last day that
  actually happened; long introductions appear from tablet width upwards.

### Fixed
- **More than 30 German strings** still showed in every language — the
  “Explore bookings” heading, the key figures (incoming, outgoing, net,
  saved/invested), the filter labels, “Top spending”, “By category”, “Why
  this category?”, several messages after saving, and the note above the
  costs that are not counted. 0.56.0 translated the templates but not the
  strings inside the JavaScript.
- A checkbox shrank to 13 pixels wide inside a flex row.

## [0.57.0] – 2026-09-20

### Added
- **Account filter on the fixed costs page** — the same control as on the
  spending page: one tick per account, *Apply*, and every figure on the
  page covers only those accounts: contracts, the per-category averages,
  the category list and both totals. A line above the title then names the
  accounts included, so a filtered total never looks like the full one.
  All ticks set means all accounts.

### Fixed
- **The running salary month was only drawn up to today.** Since 0.52.0
  the daily chart was meant to show the whole period with the days still
  to come as faint dots; in fact it stopped at today, because that is
  where the open period ends. On the third day of a salary month you saw
  three bars instead of a month. The chart now reaches the expected end of
  the salary month — the coming days still count nowhere.
- On a phone the chart consequently scrolled into a strip of empty future
  days; it now rests on the last day that has actually happened.
- **Ten German strings on the fixed costs page** were missed in 0.56.0 and
  showed in every language, among them the table heading, the saving
  badge, “loading …”, “since …”, the monthly-average note and the contract
  counter.
- **The quick date ranges on the bookings page** (“Today”, “Yesterday”,
  “Current month”, “Last 12 months”, …) were hard-coded German as well, as
  were the two prompts for creating a category.
- A first name appeared in a visible hint (the salary-month anchor day)
  in all five languages, and in 37 source comments.

## [0.56.0] – 2026-09-20

### Added
- **Saving game** on the spending page: every day that is already over scores
  points — a day without spending scores most, a day below your own benchmark
  scores some — plus a bonus of up to 20 % when the period ends in the black.
  Streaks, badges and five ranks, and a leaderboard across every month so the
  best one is obvious. Days that have not happened yet, and days for which no
  booking has arrived, count nowhere.
- **Leaderboard** of all periods, sorted by points ratio so short and running
  periods stay comparable; collapsed to the best three plus the current one.
- **Account filter** on the spending page: take single accounts out of every
  figure on the page, or look at one account alone.
- **Receipts say how they were paid** — found as a booking on an account, paid
  in cash, or still open because the statement for that day is missing. Cash
  receipts can be filed into a category, which is the only way that money shows
  up anywhere.
- **"How much is left"** — the budget alarm's figures (remaining, per day, days
  left) are shown next to the spending, and the daily chart draws the allowed
  daily amount as a line.

### Changed
- **Upload is the one way in.** Documents, receipts, statement PDFs and bank
  CSVs all go through the same page — single files, whole folders, drag and
  drop or a phone scan. The separate import form on the finance page is gone.
- **The bookings and fixed-costs pages speak all five languages**, and numbers
  and dates now follow the interface language instead of a fixed German format.
- The daily spending chart was rebuilt: outliers run off the top instead of
  flattening every other day, every bar carries its amount, days without
  spending are shown as what they are, and the whole period is visible from the
  first day of the month.

### Fixed
- A bank CSV dropped on the upload page never reached the server: the browser's
  file filter only knew documents and images and discarded it silently.
- A receipt was almost never matched to its booking, because the matcher looked
  for an amount in the OCR text instead of using the total the receipt already
  carries.
- A statement that arrived late was counted twice: the synthetic booking that
  had bridged the gap was never removed.
- A gap in the imported data was treated as a day without spending.

## [0.1.0 – 0.55.0] – 2026-03 … 2026-09

The private development history, in brief:

- **Documents**: folder watcher, OCR for scans and photos, AI classification
  with a stated reason, year/category/subcategory filing, readable file names,
  full-text search, tags, a review queue, duplicate detection, deadline
  extraction and reminders, a trash that can be undone.
- **Receipts**: line-item extraction, item categories, shop types, monthly and
  per-category analytics, a salvage pass for receipts that were filed as
  ordinary documents.
- **Money**: CSV import for nine German banks, deterministic statement-PDF
  reader verified against the statement's own balances, account management,
  net worth, savings accounts, transfers recognised by IBAN, category learning
  per payee, AI suggestions for unknown payees, fixed-cost detection, salary
  months, budget alarm, invoice-to-booking matching.
- **Platform**: multi-user login with an allowlist permission model, five
  languages, light and dark themes, mobile layout, rclone backups, one-click
  updates, Docker image, HTTPS, configurable AI provider including local
  models.
