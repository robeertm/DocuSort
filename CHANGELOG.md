# Changelog

All notable changes to DocuSort will be documented in this file.

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
