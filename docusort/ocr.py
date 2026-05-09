"""OCR and text extraction for PDFs and images.

For PDFs we use ocrmypdf, which in turn wraps Tesseract and adds a proper text
layer to the PDF. After OCR we use pypdf to pull text out.

For image files (JPG/PNG) we run Tesseract directly via pytesseract.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pypdf import PdfReader

from .config import OCRSettings


logger = logging.getLogger("docusort.ocr")


PDF_SUFFIXES = {".pdf"}
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}


@dataclass
class OcrResult:
    text: str
    path: Path          # original or OCR'd path
    ocr_used: bool = False
    page_count: int | None = None


def _extract_pdf_pages(pdf_path: Path) -> list[str]:
    """Per-page text extraction. Returns one string per page (in order),
    empty list if pypdf can't open the file."""
    try:
        reader = PdfReader(str(pdf_path))
        return [(page.extract_text() or "").strip() for page in reader.pages]
    except Exception as exc:
        logger.warning("pypdf could not read %s: %s", pdf_path.name, exc)
        return []


def _extract_pdf(pdf_path: Path) -> tuple[str, int | None]:
    pages = _extract_pdf_pages(pdf_path)
    if not pages:
        return "", None
    return "\n".join(pages).strip(), len(pages)


def _needs_ocr(text: str) -> bool:
    """A PDF is considered 'scanned' if it has almost no extractable text."""
    return len(text.strip()) < 100


# Aspect ratio threshold above which we treat a doc as a thermal-paper
# Kassenzettel (single narrow column of text). Tesseract's default
# pagesegmode=3 auto-segments such docs as if they had multiple
# columns — fusing adjacent rows, e.g. "LEERGUTRUECKNAHME -2,00" and
# the next "FANTA/SPRITE 1,49" end up on one OCR line. With
# pagesegmode=4 ("Assume a single column of text of variable sizes")
# Tesseract treats the whole page as one column, which is exactly
# what a receipt is.
_RECEIPT_ASPECT_RATIO = 2.0


def _doc_aspect_is_narrow(pdf_path: Path) -> bool:
    """Return True iff the first page of `pdf_path` is much taller
    than wide — i.e. plausibly a thermal-paper receipt rather than an
    A4 letter / multi-column document. Errors and absent pages return
    False (treat as normal doc)."""
    try:
        reader = PdfReader(str(pdf_path))
        if not reader.pages:
            return False
        page = reader.pages[0]
        # mediabox values are in PDF user units; we only care about ratio
        w = float(page.mediabox.width or 0)
        h = float(page.mediabox.height or 0)
        if w <= 0 or h <= 0:
            return False
        return (h / w) >= _RECEIPT_ASPECT_RATIO
    except Exception:
        return False


def _image_is_narrow(image_path: Path) -> bool:
    """Same heuristic for raw image files (JPG/PNG scans)."""
    try:
        from PIL import Image
        with Image.open(image_path) as img:
            w, h = img.size
        if w <= 0 or h <= 0:
            return False
        return (h / w) >= _RECEIPT_ASPECT_RATIO
    except Exception:
        return False


def _run_ocrmypdf(src: Path, dst: Path, settings: OCRSettings,
                  *, narrow_doc: bool = False) -> None:
    cmd = [
        "ocrmypdf",
        "--language", settings.languages,
        "--output-type", "pdf",
        "--skip-text" if settings.skip_if_text else "--force-ocr",
    ]
    if settings.deskew:
        cmd.append("--deskew")
    if narrow_doc:
        # Single-column-of-variable-sizes — stops Tesseract from
        # collapsing adjacent receipt rows into one line.
        cmd += ["--tesseract-pagesegmode", "4"]
    cmd += [str(src), str(dst)]

    logger.info("Running OCR: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def extract_text(file_path: Path, settings: OCRSettings) -> OcrResult:
    """Extract text and metadata from a PDF or image file.

    For scanned PDFs, returns the path to a new PDF with a proper text layer;
    callers should use that path when moving the file to the library so the
    text stays searchable.
    """
    suffix = file_path.suffix.lower()

    if suffix in PDF_SUFFIXES:
        text, pages = _extract_pdf(file_path)
        if not settings.enabled or not _needs_ocr(text):
            return OcrResult(text=text, path=file_path, ocr_used=False, page_count=pages)

        if not shutil.which("ocrmypdf"):
            logger.warning("ocrmypdf not on PATH – skipping OCR for %s", file_path.name)
            return OcrResult(text=text, path=file_path, ocr_used=False, page_count=pages)

        narrow = _doc_aspect_is_narrow(file_path)
        if narrow:
            logger.info("OCR: %s looks like a narrow Kassenzettel (h/w >= %.1f), using --psm 4",
                        file_path.name, _RECEIPT_ASPECT_RATIO)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            _run_ocrmypdf(file_path, tmp_path, settings, narrow_doc=narrow)
            ocred_text, ocred_pages = _extract_pdf(tmp_path)
            return OcrResult(
                text=ocred_text, path=tmp_path, ocr_used=True,
                page_count=ocred_pages or pages,
            )
        except subprocess.CalledProcessError as exc:
            logger.error(
                "OCR failed for %s: %s", file_path.name, exc.stderr or exc.stdout
            )
            tmp_path.unlink(missing_ok=True)
            return OcrResult(text=text, path=file_path, ocr_used=False, page_count=pages)

    if suffix in IMAGE_SUFFIXES:
        try:
            import pytesseract  # local import – only needed for images
            from PIL import Image

            narrow = _image_is_narrow(file_path)
            tess_config = "--psm 4" if narrow else ""
            if narrow:
                logger.info("OCR: %s looks like a narrow Kassenzettel image, using --psm 4",
                            file_path.name)
            text = pytesseract.image_to_string(
                Image.open(file_path), lang=settings.languages, config=tess_config,
            )
            return OcrResult(text=text.strip(), path=file_path, ocr_used=True, page_count=1)
        except Exception as exc:
            logger.error("Image OCR failed for %s: %s", file_path.name, exc)
            return OcrResult(text="", path=file_path, ocr_used=False, page_count=None)

    logger.warning("Unsupported file type: %s", file_path.suffix)
    return OcrResult(text="", path=file_path, ocr_used=False, page_count=None)


def is_supported(file_path: Path) -> bool:
    return file_path.suffix.lower() in (PDF_SUFFIXES | IMAGE_SUFFIXES)


def extract_pages(file_path: Path, settings: OCRSettings) -> list[str]:
    """Per-page text extraction for a PDF — runs OCR if it's a scanned
    PDF, otherwise reads the existing text layer page by page. Image
    files return a single-element list. Used by the page-by-page
    statement extractor."""
    suffix = file_path.suffix.lower()

    if suffix in PDF_SUFFIXES:
        pages = _extract_pdf_pages(file_path)
        joined = "\n".join(pages).strip()
        if not settings.enabled or not _needs_ocr(joined):
            return pages
        if not shutil.which("ocrmypdf"):
            return pages
        narrow = _doc_aspect_is_narrow(file_path)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            _run_ocrmypdf(file_path, tmp_path, settings, narrow_doc=narrow)
            return _extract_pdf_pages(tmp_path)
        except subprocess.CalledProcessError as exc:
            logger.error("OCR failed for %s: %s",
                         file_path.name, exc.stderr or exc.stdout)
            tmp_path.unlink(missing_ok=True)
            return pages

    if suffix in IMAGE_SUFFIXES:
        # No pages — treat the whole image as page 1.
        result = extract_text(file_path, settings)
        return [result.text] if result.text else []

    return []
