import os
import re
import glob
import platform
import hashlib
import logging
import multiprocessing
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

# Import your external ECTS engine
from utils.ocr_engine import extract_ects_ocr
from utils.claimed_dom_extract import _floatcast

try:
    from pdf2image import convert_from_path
    import pytesseract
except ImportError:
    convert_from_path = None
    pytesseract = None

# ==============================================================================
# 1. GLOBAL CONFIGURATION & ENVIRONMENT SETUP
# ==============================================================================

# CRITICAL: Limit Tesseract threads to prevent CPU explosion
os.environ["OMP_THREAD_LIMIT"] = "2"
os.environ["OMP_NUM_THREADS"] = "2"

@dataclass
class OCRConfig:
    """Central configuration for OCR."""
    DEFAULT_LANG: str = "deu+eng"
    DEFAULT_PSM: int = 6
    TIMEOUT_SECONDS: int = 100
    DPI: int = 300
    
    # System Paths (Auto-detected)
    TESSERACT_CMD: Optional[str] = None
    POPPLER_PATH: Optional[str] = None
    
    # Threading
    NUM_CPUS: int = max(1, multiprocessing.cpu_count() or 1)
    MAX_WORKERS: int = max(1, int(NUM_CPUS * 0.5))

CONFIG = OCRConfig()

NOTE_STRICT_RE = re.compile(r"\b([0-6][.,]\d{1,2})\b")

_FILE_HASH_CACHE: Dict[str, str] = {}
# Cache Key: (file_hash, dpi, psm, max_pages)

_OCR_TEXT_CACHE: Dict[tuple, str] = {}

_OCR_POOL = None

def get_ocr_pool():
    global _OCR_POOL
    if _OCR_POOL is None:
        _OCR_POOL = ProcessPoolExecutor(max_workers=CONFIG.MAX_WORKERS)
    return _OCR_POOL

# ==============================================================================
# 2. SYSTEM PATH DETECTION
# ==============================================================================

class OCRSystem:
    @staticmethod
    def setup():
        if pytesseract is None: return

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
        if env_cmd and os.path.isfile(env_cmd): return env_cmd

        system = platform.system()
        candidates = []
        if system == "Windows":
            candidates = [
                r"C:\Program Files\Tesseract-OCR\tesseract.exe",
                r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            ]
        elif system == "Darwin":
            candidates = ["/usr/local/bin/tesseract", "/opt/homebrew/bin/tesseract"]

        for c in candidates:
            if os.path.isfile(c): return c
        return None

    @staticmethod
    def _detect_poppler_path() -> Optional[str]:
        env_path = os.environ.get("POPPLER_PATH")
        if env_path and os.path.isdir(env_path): return env_path

        system = platform.system()
        candidates = []
        if system == "Windows":
            candidates = [r"C:\Program Files\poppler\bin", r"C:\Program Files (x86)\poppler\bin"]
            candidates.extend(glob.glob(r"C:\Users\*\AppData\Local\Microsoft\WinGet\Packages\*\poppler*\Library\bin"))
            for d in glob.glob(r"C:\Program Files\poppler-*"): candidates.append(os.path.join(d, "bin"))
        elif system == "Darwin":
            candidates = ["/usr/local/opt/poppler/bin", "/opt/homebrew/opt/poppler/bin"]

        for p in candidates:
            if os.path.isdir(p): return p
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
    if pdf_path in _FILE_HASH_CACHE: return _FILE_HASH_CACHE[pdf_path]
    h = hashlib.sha1()
    with open(pdf_path, "rb") as f:
        while chunk := f.read(8192): h.update(chunk)
    digest = h.hexdigest()
    _FILE_HASH_CACHE[pdf_path] = digest
    return digest

def _ocr_single_image(img, lang=CONFIG.DEFAULT_LANG, psm=CONFIG.DEFAULT_PSM, timeout=CONFIG.TIMEOUT_SECONDS, description="Unknown"):
    try:
        return pytesseract.image_to_string(img, lang=lang, config=f"--psm {psm}", timeout=timeout)
    except RuntimeError as e:
        if "timeout" in str(e).lower(): logging.warning(f"OCR Page Timeout at {description}")
        else: logging.error(f"OCR Page Error: {e}")
        return ""

def ocr_text_from_pdf(pdf_path: str, dpi: int = CONFIG.DPI, max_pages: Optional[int] = None) -> str:
    """
    Main entry point for PDF OCR. 
    max_pages: If set (e.g., 1), only OCRs the first N pages.
    """
    ensure_ocr_available()

    # Cache Key must include max_pages so a "Preview" doesn't block a "Full" request later
    file_hash = _compute_file_hash(pdf_path)
    cache_key = (file_hash, dpi, CONFIG.DEFAULT_PSM, max_pages)
    
    if cache_key in _OCR_TEXT_CACHE:
        return _OCR_TEXT_CACHE[cache_key]

    log_msg = f"Start OCR for {os.path.basename(pdf_path)} (dpi={dpi}"
    if max_pages: log_msg += f", pages={max_pages}"
    log_msg += ")"
    logging.debug(log_msg)

    # Convert PDF to Images
    try:
        # pdf2image uses 'last_page' parameter to limit processing
        images = convert_from_path(
            pdf_path, 
            dpi=dpi, 
            poppler_path=CONFIG.POPPLER_PATH,
            last_page=max_pages
        )
    except Exception as e:
        logging.error(f"pdf2image failed for {pdf_path}: {e}")
        return ""

    # Run OCR
    text_parts = []
    for img in images:
        # We call the helper directly. No Executor overhead, as we do parallelism outside.
        text = _ocr_single_image(img, description=os.path.basename(pdf_path))
        text_parts.append(text)

    full_text = "\n".join(text_parts)
    _OCR_TEXT_CACHE[cache_key] = full_text
    return full_text


# ==============================================================================
# 4. BUSINESS LOGIC (Evaluation/Extraction)
# ==============================================================================

_GRADE_KEYWORDS = [
    "gesamtnote", "abschlussnote", "abschlusspruefung", "abschlussprüfung",
    "average mark", "overall grade", "overall result", "overall mark",
    "final grade", "final result", "gesamturteil", "gesamtbewertung",
    "gesamtprädikat", "gesamtpraedikat", "gesamtleistung"
]
_GRADE_CONTEXT_RE = re.compile(
    "|".join(re.escape(k) for k in _GRADE_KEYWORDS), 
    re.IGNORECASE
)

def extract_ocr_note(text: str) -> Optional[float]:
    if not text: return None
    for ln in (line.strip() for line in text.splitlines()):
        if not ln:
            continue
        if not _GRADE_CONTEXT_RE.search(ln):
            continue
        m = NOTE_STRICT_RE.search(ln)
        if m:
            try:
                val = _floatcast(m.group(1))
                logging.debug(f"OCR-Note found: {val} in line '{ln[:50]}...'")
                return val
            except ValueError: continue
    return None

async def extract_ects_hybrid_async(pdf_path, module_map, categories) -> Tuple[Dict, List, List, str]:
    """
    Async version of extract_ects_hybrid.
    Runs the heavy blocking OCR function in a separate process.
    """
    if not os.path.exists(pdf_path):
        logging.error(f"File not found: {pdf_path}")
        return {cat: 0.0 for cat in categories}, [], [], "ocr_hocr"

    logging.debug(f"Hybrid OCR Extraction (Async) started: {os.path.basename(pdf_path)}")
    
    loop = asyncio.get_running_loop()
    pool = get_ocr_pool()

    try:
        sums, matched_modules, unrecognized, method = await asyncio.wait_for(
            loop.run_in_executor(
                pool, 
                extract_ects_ocr,
                pdf_path, 
                module_map, 
                categories
            ),
            timeout=CONFIG.TIMEOUT_SECONDS
        )
        
        logging.debug(f"OCR Finished ({method}), Sum: {sum(sums.values())}")
        return sums, matched_modules, unrecognized, method

    except asyncio.TimeoutError:
        logging.warning(f"OCR Timeout (> {CONFIG.TIMEOUT_SECONDS}s): {os.path.basename(os.path.dirname(pdf_path))}/{os.path.basename(pdf_path)}")
        return {}, [], [], "FAILED_TIMEOUT"
        
    except Exception as e:
        logging.error(f"Hybrid OCR Async Error: {e}")
        return {}, [], [], "FAILED_ERROR"
