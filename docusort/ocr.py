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
from pathlib import Path

from pypdf import PdfReader

from .config import OCRSettings


logger = logging.getLogger("docusort.ocr")


PDF_SUFFIXES = {".pdf"}
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}


def _extract_pdf_text(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
        chunks = []
        for page in reader.pages:
            text = page.extract_text() or ""
            chunks.append(text)
        return "\n".join(chunks).strip()
    except Exception as exc:
        logger.warning("pypdf could not read %s: %s", pdf_path.name, exc)
        return ""


def _needs_ocr(text: str) -> bool:
    """A PDF is considered 'scanned' if it has almost no extractable text."""
    return len(text.strip()) < 100


def _run_ocrmypdf(src: Path, dst: Path, settings: OCRSettings) -> None:
    cmd = [
        "ocrmypdf",
        "--language", settings.languages,
        "--output-type", "pdf",
        "--skip-text" if settings.skip_if_text else "--force-ocr",
    ]
    if settings.deskew:
        cmd.append("--deskew")
    cmd += [str(src), str(dst)]

    logger.info("Running OCR: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def extract_text(file_path: Path, settings: OCRSettings) -> tuple[str, Path]:
    """Return (text, possibly_ocred_path).

    The second value is either the original file or, for scanned PDFs, a new
    PDF with a proper text layer. The caller is responsible for using that
    path when moving the file to the library so the text stays searchable.
    """
    suffix = file_path.suffix.lower()

    if suffix in PDF_SUFFIXES:
        text = _extract_pdf_text(file_path)
        if not settings.enabled or not _needs_ocr(text):
            return text, file_path

        if not shutil.which("ocrmypdf"):
            logger.warning("ocrmypdf not on PATH – skipping OCR for %s", file_path.name)
            return text, file_path

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            _run_ocrmypdf(file_path, tmp_path, settings)
            ocred_text = _extract_pdf_text(tmp_path)
            return ocred_text, tmp_path
        except subprocess.CalledProcessError as exc:
            logger.error(
                "OCR failed for %s: %s", file_path.name, exc.stderr or exc.stdout
            )
            tmp_path.unlink(missing_ok=True)
            return text, file_path

    if suffix in IMAGE_SUFFIXES:
        try:
            import pytesseract  # local import – only needed for images
            from PIL import Image

            text = pytesseract.image_to_string(
                Image.open(file_path), lang=settings.languages
            )
            return text.strip(), file_path
        except Exception as exc:
            logger.error("Image OCR failed for %s: %s", file_path.name, exc)
            return "", file_path

    logger.warning("Unsupported file type: %s", file_path.suffix)
    return "", file_path


def is_supported(file_path: Path) -> bool:
    return file_path.suffix.lower() in (PDF_SUFFIXES | IMAGE_SUFFIXES)
