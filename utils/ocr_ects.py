import os
import re
import glob
import platform
import hashlib
import logging
import multiprocessing
import concurrent.futures
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

# External libs
from func_timeout import func_timeout, FunctionTimedOut

try:
    from pdf2image import convert_from_path
    import pytesseract
except ImportError:
    convert_from_path = None
    pytesseract = None

# Import your external ECTS engine
from utils.ocr_engine import extract_ects_ocr

# ==============================================================================
# 1. GLOBAL CONFIGURATION & ENVIRONMENT SETUP
# ==============================================================================

# Limit Tesseract threads to prevent CPU explosion
os.environ["OMP_THREAD_LIMIT"] = "2"
os.environ["OMP_NUM_THREADS"] = "2"


@dataclass
class OCRConfig:
    """Central configuration for OCR."""
    DEFAULT_LANG: str = "deu+eng"
    DEFAULT_PSM: int = 6
    TIMEOUT_SECONDS: int = 60
    DPI: int = 200

    # System Paths (Auto-detected)
    TESSERACT_CMD: Optional[str] = None
    POPPLER_PATH: Optional[str] = None

    # Threading
    NUM_CPUS: int = max(1, multiprocessing.cpu_count() or 1)
    MAX_WORKERS: int = max(1, int(NUM_CPUS * 0.8))


CONFIG = OCRConfig()

NOTE_STRICT_RE = re.compile(r"\b([0-6][.,]\d{1,2})\b")

_FILE_HASH_CACHE: Dict[str, str] = {}
# Cache Key: (file_hash, dpi, psm, max_pages)
_OCR_TEXT_CACHE: Dict[tuple, str] = {}


# ==============================================================================
# 2. SYSTEM PATH DETECTION
# ==============================================================================

class OCRSystem:
    @staticmethod
    def setup():
        if pytesseract is None:
            return

        # Tesseract
        tess_path = OCRSystem._detect_tesseract_path()
        if tess_path:
            pytesseract.pytesseract.tesseract_cmd = tess_path
            CONFIG.TESSERACT_CMD = tess_path
        else:
            pytesseract.pytesseract.tesseract_cmd = "tesseract"
            CONFIG.TESSERACT_CMD = "tesseract"

        # Poppler
        CONFIG.POPPLER_PATH = OCRSystem._detect_poppler_path()

    @staticmethod
    def _detect_tesseract_path() -> Optional[str]:
        env_cmd = os.environ.get("TESSERACT_CMD")
        if env_cmd and os.path.isfile(env_cmd):
            return env_cmd

        system = platform.system()
        candidates: List[str] = []
        if system == "Windows":
            candidates = [
                r"C:\Program Files\Tesseract-OCR\tesseract.exe",
                r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            ]
        elif system == "Darwin":
            candidates = ["/usr/local/bin/tesseract", "/opt/homebrew/bin/tesseract"]

        for c in candidates:
            if os.path.isfile(c):
                return c
        return None

    @staticmethod
    def _detect_poppler_path() -> Optional[str]:
        env_path = os.environ.get("POPPLER_PATH")
        if env_path and os.path.isdir(env_path):
            return env_path

        system = platform.system()
        candidates: List[str] = []
        if system == "Windows":
            candidates = [
                r"C:\Program Files\poppler\bin",
                r"C:\Program Files (x86)\poppler\bin",
            ]
            candidates.extend(
                glob.glob(
                    r"C:\Users\*\AppData\Local\Microsoft\WinGet\Packages\*\poppler*\Library\bin"
                )
            )
            for d in glob.glob(r"C:\Program Files\poppler-*"):
                candidates.append(os.path.join(d, "bin"))
        elif system == "Darwin":
            candidates = [
                "/usr/local/opt/poppler/bin",
                "/opt/homebrew/opt/poppler/bin",
            ]

        for p in candidates:
            if os.path.isdir(p):
                return p
        return None


OCRSystem.setup()


# ==============================================================================
# 3. CORE OCR FUNCTIONALITY
# ==============================================================================

def ensure_ocr_available():
    if convert_from_path is None or pytesseract is None:
        raise RuntimeError("OCR libraries missing (pdf2image/pytesseract).")
    return True


def _compute_file_hash(pdf_path: str) -> str:
    if pdf_path in _FILE_HASH_CACHE:
        return _FILE_HASH_CACHE[pdf_path]
    h = hashlib.sha1()
    with open(pdf_path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    digest = h.hexdigest()
    _FILE_HASH_CACHE[pdf_path] = digest
    return digest


def _ocr_single_image(
    img,
    lang: str = CONFIG.DEFAULT_LANG,
    psm: int = CONFIG.DEFAULT_PSM,
    timeout: int = CONFIG.TIMEOUT_SECONDS,
) -> str:
    try:
        return pytesseract.image_to_string(
            img, lang=lang, config=f"--psm {psm}", timeout=timeout
        )
    except RuntimeError as e:
        if "timeout" in str(e).lower():
            logging.warning("OCR Page Timeout")
        else:
            logging.error(f"OCR Page Error: {e}")
        return ""
    except Exception as e:
        logging.error(f"General OCR Error: {e}")
        return ""


def ocr_text_from_pdf(
    pdf_path: str,
    dpi: int = CONFIG.DPI,
    max_pages: Optional[int] = None,
) -> str:
    """
    Main entry point for PDF OCR.
    max_pages: If set (e.g., 1), only OCRs the first N pages.
    """
    ensure_ocr_available()

    # Cache key includes max_pages so a preview OCR does not block a full run later
    file_hash = _compute_file_hash(pdf_path)
    cache_key = (file_hash, dpi, CONFIG.DEFAULT_PSM, max_pages)

    if cache_key in _OCR_TEXT_CACHE:
        return _OCR_TEXT_CACHE[cache_key]

    log_msg = f"Start OCR for {os.path.basename(pdf_path)} (dpi={dpi}"
    if max_pages:
        log_msg += f", pages={max_pages}"
    log_msg += ")"
    logging.info(log_msg)

    # Convert PDF to images
    try:
        images = convert_from_path(
            pdf_path,
            dpi=dpi,
            poppler_path=CONFIG.POPPLER_PATH,
            last_page=max_pages,
        )
    except Exception as e:
        logging.error(f"pdf2image failed for {pdf_path}: {e}")
        return ""

    # Run parallel OCR
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(len(images), CONFIG.MAX_WORKERS)
    ) as executor:
        results = executor.map(
            _ocr_single_image,
            images,
            timeout=CONFIG.TIMEOUT_SECONDS,
        )
        text_parts = list(results)

    full_text = "\n".join(text_parts)
    _OCR_TEXT_CACHE[cache_key] = full_text
    return full_text


# ==============================================================================
# 4. BUSINESS LOGIC (Evaluation/Extraction)
# ==============================================================================

def extract_ocr_note(text: str) -> Optional[float]:
    if not text:
        return None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    keywords = {
        "gesamtnote",
        "abschlussnote",
        "abschlusspruefung",
        "abschlussprüfung",
        "average mark",
        "overall grade",
        "overall result",
        "overall mark",
        "final grade",
        "final result",
        "gesamturteil",
        "gesamtbewertung",
        "gesamtprädikat",
        "gesamtpraedikat",
        "gesamtleistung",
    }

    for ln in lines:
        if not any(kw in ln.lower() for kw in keywords):
            continue
        m = NOTE_STRICT_RE.search(ln)
        if m:
            try:
                val = float(m.group(1).replace(",", "."))
                logging.debug(
                    f"OCR-Note found: {val} in line '{ln[:50]}...'"
                )
                return val
            except ValueError:
                continue
    return None


MODULE_OVERVIEW_ECTS_RE = re.compile(
    r'(\d{1,2}(?:[.,]\d)?)\s*(?:ects|lp|credit points?|cp)\b',
    re.IGNORECASE
)

def _extract_ects_from_module_overview(
    pdf_path: str,
    module_map: Dict[str, str],
    categories: List[str],
) -> Tuple[Dict[str, float], List[str], List[str], str]:
    text = ocr_text_from_pdf(pdf_path)
    if not text.strip():
        return {cat: 0.0 for cat in categories}, [], [], "module_overview_empty"

    sums: Dict[str, float] = {cat: 0.0 for cat in categories}
    matched_modules: List[str] = []
    unrecognized_lines: List[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        low = line.lower()

        if not any(ch.isdigit() for ch in low):
            continue

        found_mod = None
        found_cat = None
        for mod_name, cat in module_map.items():
            if mod_name.lower() in low:
                found_mod = mod_name
                found_cat = cat
                break

        if not found_cat:
            unrecognized_lines.append(line)
            continue

        m = MODULE_OVERVIEW_ECTS_RE.search(low)
        if m:
            val_str = m.group(1)
        else:
            nums = re.findall(r'(\d{1,2}(?:[.,]\d)?)', low)
            if not nums:
                unrecognized_lines.append(line)
                continue
            val_str = nums[-1]

        try:
            ects_val = float(val_str.replace(",", "."))
        except ValueError:
            unrecognized_lines.append(line)
            continue

        if found_cat not in sums:
            sums[found_cat] = 0.0
        sums[found_cat] += ects_val

        matched_modules.append(
            f"{found_mod} -> {found_cat}:{ects_val} | {line}"
        )

    return sums, matched_modules, unrecognized_lines, "module_overview_simple"


def extract_ects_hybrid(
    pdf_path: str,
    module_map: Dict[str, str],
    categories: List[str],
    doc_type: str = "auto",
) -> Tuple[Dict[str, float], List[str], List[str], str]:
    """
    Hybrid ECTS extraction:
        - doc_type == "module_overview"  -> simple overview parser
        - doc_type == "transcript"       -> full hOCR engine
        - doc_type == "fallback"/other   -> full hOCR engine

    Returns:
        (sums_per_category, matched_modules, unrecognized_lines, method_label)
    """
    if not os.path.exists(pdf_path):
        logging.error(f"File not found: {pdf_path}")
        return {cat: 0.0 for cat in categories}, [], [], "FAILED_NOFILE"

    # Dedicated path for module overviews
    if doc_type == "module_overview":
        logging.info(
            f"Module-overview ECTS extraction: {os.path.basename(pdf_path)}"
        )
        sums, matched_modules, unrecognized, method = _extract_ects_from_module_overview(
            pdf_path, module_map, categories
        )
        logging.info(f"OCR Finished ({method}), Sum: {sum(sums.values())}")
        return sums, matched_modules, unrecognized, method

    # Default / transcript / fallback -> use existing hOCR engine
    logging.info(f"Hybrid OCR Extraction started: {os.path.basename(pdf_path)}")
    try:
        sums, matched_modules, unrecognized, method = func_timeout(
            CONFIG.TIMEOUT_SECONDS,
            extract_ects_ocr,
            args=(pdf_path, module_map, categories),
        )
    except FunctionTimedOut:
        logging.warning(
            f"Hybrid OCR Timeout (> {CONFIG.TIMEOUT_SECONDS}s): "
            f"{os.path.basename(pdf_path)}"
        )
        return {cat: 0.0 for cat in categories}, [], [], "FAILED_TIMEOUT"
    except Exception as e:
        logging.error(f"Hybrid OCR Error: {e}")
        return {cat: 0.0 for cat in categories}, [], [], "FAILED_ERROR"

    logging.info(f"OCR Finished ({method}), Sum: {sum(sums.values())}")
    return sums, matched_modules, unrecognized, method