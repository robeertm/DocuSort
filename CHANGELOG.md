# Changelog

All notable changes to DocuSort are documented here.
Dates are ISO; versions follow `MAJOR.MINOR.PATCH`.

This file starts with the first public release. The project was developed
privately before that; the summary under *0.1.0 – 0.55.0* lists what arrived
along the way rather than every single step.

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
