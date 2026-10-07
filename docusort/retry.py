"""Re-classify a document — from review, from failed, or simply filed wrong.

Uses the stored extracted text so no OCR is paid for twice; only an empty
text falls back to a fresh OCR run. On success the file is moved to its new
category folder and the DB row is updated in place, keeping the same `id`
and accumulating token usage.

🔑 The stored text is kept in full (200 000 characters, as the main
pipeline does). How much of it the model gets to see is decided when the
prompt is built — never by what the database happens to hold.

🔑 A document that turns out to be a bank statement is handed to the
same reader the main pipeline uses (`finance.pdf_statement`), so the same
file produces the same bookings whichever way it got here.
"""

from __future__ import annotations
from .kategorien import ist as _kat_ist

import logging
import shutil
from pathlib import Path
from typing import Any

from .classifier import Classifier
from .config import AppSettings
from .db import Database
from .ocr import extract_text
from .organizer import _parse_iso_date, build_filename, _uniquify  # type: ignore
from .i18n import uebersetze_jetzt as _u


logger = logging.getLogger("docusort.retry")


def _sieht_wie_bank_aus(cls) -> bool:
    """Ein Dokument, das das Modell „Bank" nennt und das nach einem Auszug
    klingt. Die Kategorie allein reicht nicht: „Bank" traegt auch jeder
    Werbebrief einer Bank."""
    if cls.category != "Bank":
        return False
    subj = (cls.subject or "").lower()
    return ((cls.subcategory or "").lower() in ("konto", "karte")
            or any(w in subj for w in ("kontoauszug", "girokonto",
                                       "tagesgeld", "kreditkart")))


def retry_document(
    doc_id: int,
    settings: AppSettings,
    classifier: Classifier,
    db: Database,
) -> dict[str, Any]:
    doc = db.get(doc_id)
    if not doc:
        raise ValueError(_u("err.doc_missing", doc_id=doc_id))

    # 🔑 Den Stand VORHER festhalten. Ohne ihn kann die Oberflaeche
    #    nicht zwischen „das Modell sagt etwas Neues" und „das Modell sagt
    #    dasselbe wie vorher" unterscheiden — und genau das sah fuer den
    #    Benutzer aus wie ein Knopf, der nichts tut.
    vorher_kategorie = doc.get("category") or ""
    vorher_unter = doc.get("subcategory") or ""
    vorher_status = doc.get("status") or ""

    text = doc.get("extracted_text") or ""
    if not text:
        # extracted_text was never stored — re-OCR from whichever file we still have.
        source = Path(doc.get("library_path") or doc.get("processed_path") or "")
        if not source.exists():
            raise ValueError(_u("err.source_file_missing", pfad=source))
        logger.info("retry %d: no stored text, re-OCRing %s", doc_id, source)
        ocr_res = extract_text(source, settings.ocr)
        text = ocr_res.text

    if not text:
        raise ValueError(_u("err.no_text_in_file"))

    cls = classifier.classify(text)
    logger.info(
        "retry %d classified -> %s / %s (conf=%.2f, $%.4f)",
        doc_id, cls.category, cls.date, cls.confidence, cls.cost_usd,
    )

    # Move the file to its new home.
    current = Path(doc["library_path"])
    if not current.exists():
        raise ValueError(_u("err.library_file_missing", pfad=current))

    year = _parse_iso_date(cls.date).strftime("%Y")
    if cls.is_confident:
        target_dir = settings.paths.library / year / cls.category
        if cls.subcategory:
            target_dir = target_dir / cls.subcategory
    else:
        target_dir = settings.paths.review
    target_dir.mkdir(parents=True, exist_ok=True)
    target = _uniquify(
        target_dir / build_filename(
            cls, settings.filename_template, settings.max_filename_length,
            current.suffix,
        )
    )
    shutil.move(str(current), str(target))

    status = "filed" if cls.is_confident else "review"
    db.update_classification(
        doc_id, cls,
        library_path=str(target),
        filename=target.name,
        status=status,
        # 🔴 DEN GANZEN TEXT BEHALTEN, nicht auf die Modellgrenze kuerzen.
        #    Hier stand `text[: settings.claude.max_text_chars]`. Das kuerzte
        #    genau das weg, was die Hauptverarbeitung ABSICHTLICH behaelt
        #    (main.py speichert 200 000 Zeichen, mit Begruendung) — und seit
        #    `max_text_chars = 0` „so viel, wie das Modell kann" heisst, war
        #    `text[:0]` LEER: jeder Klick auf „Neu klassifizieren" loeschte den
        #    gespeicherten Text des Dokuments. Damit war auch das Versprechen
        #    des Knopfes hin, beim naechsten Mal ohne neue Texterkennung
        #    auszukommen. Wieviel das Modell SIEHT, entscheidet der
        #    Klassifizierer beim Senden; die Datenbank hat damit nichts zu tun.
        extracted_text=text[:200_000],
    )

    # 🔴 HIER STAND EIN ZWEITER KONTOAUSZUG-LESER — UND ER KONNTE NIE LAUFEN.
    #    Der Block rief `_extract_statement_inline()`, und das Erste, was die
    #    Funktion tat, war `from .finance import StatementExtractor`. Diese
    #    Klasse gibt es im ganzen Projekt NICHT. Der Import scheiterte also
    #    jedes Mal, und ein `except Exception` darueber schrieb eine Warnung
    #    ins Protokoll — 82 Zeilen, die aussahen wie eine Funktion und nie
    #    eine waren. Mitgeschleppt wurden dabei die einzigen Durchsetzungen
    #    von `finance.local_only` und `finance.review_before_send`: Schalter,
    #    die nur einen Weg bewachten, den es nicht gab.
    #
    # 🔑 EIN LESER, ZWEI AUFRUFER. Die Hauptverarbeitung liest Auszuege
    #    mit `import_statement_file` — ein Textparser, der die Buchungen
    #    gegen Anfangs- und Schlussbestand des Auszugs selbst prueft und
    #    dabei NICHTS an ein Modell schickt. Damit erledigt sich die
    #    Datenschutzfrage an dieser Stelle von selbst, statt von zwei
    #    Schaltern bewacht zu werden. Genau derselbe Aufruf steht jetzt hier,
    #    mit denselben Nacharbeiten (neu einsortieren, Auszugsdokumente
    #    aufraeumen) — wer neu klassifiziert, bekommt dasselbe Ergebnis wie
    #    wer die Datei neu einwirft.
    statement_result: dict[str, Any] | None = None
    if _kat_ist(cls.category, "kontoauszug") or _sieht_wie_bank_aus(cls):
        try:
            from .finance.pdf_statement import (import_statement_file,
                                                tidy_statement_documents)
            rep, st = import_statement_file(
                db, target, doc_id=doc_id,
                file_hash=str(doc.get("content_hash") or ""),
            )
            if st is not None:
                statement_result = {
                    "statement_no": st.statement_no,
                    "account_kind": st.account_kind,
                    "transactions": rep.rows_inserted,
                    "overlap": rep.rows_overlap,
                    "skipped": rep.statements_skipped,
                    "errors": list(rep.errors),
                }
                logger.info(
                    "retry %d: Kontoauszug %s (%s): %d Buchungen neu, "
                    "%d Ueberschneidungen", doc_id, st.statement_no,
                    st.account_kind, rep.rows_inserted, rep.rows_overlap,
                )
                if rep.statements:
                    try:
                        db.finance_reclassify(force=False)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("retry %d: reclassify failed: %s", doc_id, exc)
                    try:
                        tidy_statement_documents(db, settings, log=logger,
                                                 only_doc_ids={doc_id})
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("retry %d: tidy failed: %s", doc_id, exc)
            else:
                # Kein Auszug, den dieser Leser versteht. Das ist kein Fehler
                # des Knopfes — und es wird gesagt, nicht verschwiegen.
                statement_result = {"transactions": 0, "unreadable": True,
                                    "errors": list(rep.errors)}
                logger.info("retry %d: als Kontoauszug eingeordnet, aber der "
                            "Auszugsleser erkennt die Vorlage nicht", doc_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("retry %d: Kontoauszug-Import fehlgeschlagen: %s",
                           doc_id, exc)

    return {
        "doc_id": doc_id,
        "status": status,
        "category": cls.category,
        "subcategory": cls.subcategory or "",
        "confidence": cls.confidence,
        "cost_usd": cls.cost_usd,
        "category_before": vorher_kategorie,
        "subcategory_before": vorher_unter,
        "status_before": vorher_status,
        "changed": (cls.category != vorher_kategorie
                    or (cls.subcategory or "") != vorher_unter),
        "library_path": str(target),
        "statement": statement_result,
    }


