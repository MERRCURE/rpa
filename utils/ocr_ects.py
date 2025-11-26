import os
import re
import glob
import platform
import time
import hashlib
import multiprocessing
from multiprocessing.pool import ThreadPool
from func_timeout import func_timeout, FunctionTimedOut

try:
    from pdf2image import convert_from_path
    import pytesseract
except Exception:
    convert_from_path = None
    pytesseract = None

from utils.ocr_engine import extract_ects_ocr


NOTE_STRICT_RE = re.compile(r"\b([0-6][.,]\d{1,2})\b")

_FILE_HASH_CACHE = {}
_OCR_TEXT_CACHE = {}

_NUM_CPUS = max(1, multiprocessing.cpu_count() or 1)
_MAX_THREADS = max(1, int(_NUM_CPUS * 0.9))


def _compute_file_hash(pdf_path: str) -> str:
    h_cached = _FILE_HASH_CACHE.get(pdf_path)
    if h_cached:
        return h_cached

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


def detect_tesseract():

    if pytesseract is None:
        return

    env_cmd = os.environ.get("TESSERACT_CMD")
    if env_cmd and os.path.isfile(env_cmd):
        pytesseract.pytesseract.tesseract_cmd = env_cmd
        print(f"error: Tesseract via environment: {env_cmd}")
        return

    system = platform.system()

    if system == "Windows":
        candidates = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
        for c in candidates:
            if os.path.isfile(c):
                pytesseract.pytesseract.tesseract_cmd = c
                print(f"INFO: Tesseract auto-detected: {c}")
                return
        print("error: Tesseract not found  (Windows).")

    elif system == "Darwin":
        candidates = [
            "/usr/local/bin/tesseract",
            "/opt/homebrew/bin/tesseract",
        ]
        for c in candidates:
            if os.path.isfile(c):
                pytesseract.pytesseract.tesseract_cmd = c
                print(f"INFO: Tesseract auto-detected: {c}")
                return
        print("error: Tesseract not found  (macOS).")

    else:

        pytesseract.pytesseract.tesseract_cmd = "tesseract"


def get_poppler_path():
    """
    Cross-platform Poppler detection.
    Priority:
    1. Environment variable POPPLER_PATH
    2. Windows winget/installer locations
    3. macOS Homebrew
    4. Linux: None (use system PATH)
    """
    env_path = os.environ.get("POPPLER_PATH")
    if env_path and os.path.isdir(env_path):
        print(f"INFO: Poppler path from environment: {env_path}")
        return env_path

    system = platform.system()

    if system == "Windows":
        possible_win_paths = [
            r"C:\Program Files\poppler\bin",
            r"C:\Program Files (x86)\poppler\bin",
        ]

        winget_dirs = glob.glob(
            r"C:\Users\*\AppData\Local\Microsoft\WinGet\Packages\*\poppler*\Library\bin"
        )
        possible_win_paths.extend(winget_dirs)

        program_files_dirs = glob.glob(r"C:\Program Files\poppler-*")
        for d in program_files_dirs:
            possible_win_paths.append(os.path.join(d, "bin"))

        for p in possible_win_paths:
            if os.path.isdir(p):
                print(f"INFO: Poppler found: {p}")
                return p

        print("WARNUNG: Poppler not found on Windows")
        return None

    if system == "Darwin":
        brew_paths = [
            "/usr/local/opt/poppler/bin",
            "/opt/homebrew/opt/poppler/bin",
        ]
        for p in brew_paths:
            if os.path.isdir(p):
                print(f"INFO: Poppler found: {p}")
                return p
        return None

    if system == "Linux":

        return None

    return None


if pytesseract is not None:
    detect_tesseract()
POPPLER_PATH = get_poppler_path()


def ensure_ocr_available():
    if convert_from_path is None or pytesseract is None:
        raise RuntimeError(
            "OCR not avialble   (pdf2image/pytesseract )."
        )
    return True


def _ocr_text_from_pdf_cached(pdf_path: str, dpi: int = 200, psm: int = 6) -> str:
    if convert_from_path is None or pytesseract is None:
        raise RuntimeError("OCR not available (pdf2image/pytesseract ).")

    file_hash = _compute_file_hash(pdf_path)
    cache_key = (file_hash, dpi, psm)
    if cache_key in _OCR_TEXT_CACHE:
        return _OCR_TEXT_CACHE[cache_key]

    print(f"Start OCR for {pdf_path} (dpi={dpi}, psm={psm})")
    images = convert_from_path(pdf_path, dpi=dpi, poppler_path=POPPLER_PATH)

    config = f"--psm {psm}"

    def _ocr_page(img):
        try:
            return pytesseract.image_to_string(
                img, lang="deu+eng", config=config
            )
        except Exception as e:
            print(f"OCR-error  {pdf_path}: {e}")
            return ""

    with ThreadPool(min(len(images), _MAX_THREADS)) as pool:
        text_parts = pool.map(_ocr_page, images)

    full_text = "\n".join(text_parts)
    _OCR_TEXT_CACHE[cache_key] = full_text
    return full_text


def ocr_text_from_pdf(pdf_path, dpi=200):
    return _ocr_text_from_pdf_cached(pdf_path, dpi=dpi, psm=6)


def extract_ocr_note(text: str):
    if not text:
        print("error: extract_ocr_note()")
        return None

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    keywords = [
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
    ]

    for ln in lines:
        low = ln.lower()
        if not any(kw in low for kw in keywords):
            continue

        m = NOTE_STRICT_RE.search(ln)
        if m:
            try:
                val = float(m.group(1).replace(",", "."))
                print(f"DEBUG: OCR-Note found in '{ln[:80]}...' -> {val}")
                return val
            except ValueError:
                continue

    print("error no keywords found")
    return None


def _infer_program_from_categories(categories):
    for c in categories:
        if str(c).strip().lower() == "mathematik":
            return "ai"
    return "bwl"


def extract_ects_hybrid(pdf_path, module_map, categories):
    if not os.path.exists(pdf_path):
        print(f"error: extract not found: {pdf_path}")
        return {cat: 0.0 for cat in categories}, [], [], "ocr_hocr"

    print(f"starte OCR {os.path.basename(pdf_path)}")
    try:
        # Run extract_ects_ocr with a 5-second hard limit
        sums, matched_modules, unrecognized, method = func_timeout(
            5,
            extract_ects_ocr,
            args=(pdf_path, module_map, categories)
        )
    except FunctionTimedOut:
        print(
            f"OCR abgebrochen (Timeout > 5s) für {os.path.basename(pdf_path)}")
        # Set default empty values so your code doesn't crash later
        sums, matched_modules, unrecognized, method = (
            {}, [], [], "FAILED_TIMEOUT")
    except Exception as e:
        print(f"OCR Error: {e}")
        sums, matched_modules, unrecognized, method = (
            {}, [], [], "FAILED_ERROR")

    print(f"ocr finished with {method}, sum {sum(sums.values())}")
    return sums, matched_modules, unrecognized, method
