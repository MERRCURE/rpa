#!/usr/bin/env python3
import os
import re
import logging
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher

from PIL import Image
Image.MAX_IMAGE_PIXELS = 933120000
from pdf2image import convert_from_path
import pytesseract
from pytesseract import Output

import os
import platform
import pytesseract

#XXX not very satisfying to have the config twice ...
CPU_THREADS = os.cpu_count() or 1
OCR_THREADS = max(1, int(CPU_THREADS * 0.8))

OPTIMAL_DPI = 300 # tesseract is optimized for 300 dpi, do not do more, maybe less for time?
OPTIMAL_PSM = 6  # 6 = Assume a single uniform block of text (good for tables/lines)

logging.info(
    f"OCR engine detected {CPU_THREADS} CPU threads, "
    f"using up to {OCR_THREADS} threads for page-level OCR."
)

POPPLER_PATH = os.environ.get("POPPLER_PATH", None)

tess_env_cmd = os.environ.get("TESSERACT_CMD")


def configure_tesseract():
    env_cmd = os.environ.get("TESSERACT_CMD")

    if env_cmd and os.path.exists(env_cmd):
        pytesseract.pytesseract.tesseract_cmd = env_cmd
        logging.info(f"Using TESSERACT_CMD={env_cmd}")
        return

    system = platform.system()

    if system == "Linux":
        found = shutil.which("tesseract")
        if found:
            pytesseract.pytesseract.tesseract_cmd = found
            logging.info(f"Found Tesseract on Linux at {found}")
            return

    if system == "Darwin":
        possible = [
            "/usr/local/bin/tesseract",
            "/opt/homebrew/bin/tesseract",
        ]
        for p in possible:
            if os.path.exists(p):
                pytesseract.pytesseract.tesseract_cmd = p
                logging.info(f"Found Tesseract on macOS at {p}")
                return
        # fallback: try PATH
        found = shutil.which("tesseract")
        if found:
            pytesseract.pytesseract.tesseract_cmd = found
            logging.info(f"Found Tesseract via PATH at {found}")
            return

    if system == "Windows":
        possible = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
        for p in possible:
            if os.path.exists(p):
                pytesseract.pytesseract.tesseract_cmd = p
                logging.info(f"Found Tesseract on Windows at {p}")
                return

        found = shutil.which("tesseract")
        if found:
            pytesseract.pytesseract.tesseract_cmd = found
            logging.info(f"Found Tesseract on Windows via PATH at {found}")
            return

    raise RuntimeError(
        "Tesseract not found. Please install Tesseract or set TESSERACT_CMD."
    )



UMLAUT_PATTERN = re.compile(r"[äöüß]")
NON_ALPHANUM_PATTERN = re.compile(r"[^a-z0-9\s]")
WHITESPACE_PATTERN = re.compile(r"\s+")
UMLAUT_MAP = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
}
def normalize_text(s: str) -> str:
    s = s.lower()
    s = UMLAUT_PATTERN.sub(lambda match: UMLAUT_MAP[match.group(0)], s)
    s = NON_ALPHANUM_PATTERN.sub(" ", s)
    s = WHITESPACE_PATTERN.sub(" ", s).strip()
    return s

TRASH_PATTERN = re.compile(r"[A-Za-zÄÖÜäöüß]{4,}")
TRASH_PATTERN_2 = re.compile(r"\d")
def is_trash_line(line: str) -> bool:

    if not line or line.isspace():
        return True

    if TRASH_PATTERN.search(line):
        return False

    if TRASH_PATTERN_2.search(line):
        return True

    return False


NOTE_RE = re.compile(r"\b([0-6][.,]\d+)\b")
NUM_RE = re.compile(r"^\d+(?:[.,]\d+)?$")

Module = namedtuple("Module", ["name", "category", "ects"])
FUZZY_THRESHOLD = 0.80


def _ocr_page_to_lines_and_grid(args): #XXX deprecated

    img, dpi, psm = args
    config = f"--psm {psm}"

    text = pytesseract.image_to_string(img, lang="deu+eng", config=config)
    lines = [ln.strip() for ln in text.splitlines() if not ln.isspace()]
    notes = NOTE_RE.findall(text)

    data = pytesseract.image_to_data(
        img, lang="deu+eng", config=config, output_type=Output.DICT
    )

    n = len(data["text"])
    tokens = []
    for i in range(n):
        txt = data["text"][i]
        if not txt or txt.isspace() or txt.strip() == "|":
            continue
        x = data["left"][i]
        y = data["top"][i]
        w = data["width"][i]
        h = data["height"][i]
        tokens.append(
            {
                "text": txt,
                "x": x,
                "y": y,
                "w": w,
                "h": h,
                "xc": x + w / 2.0,
                "yc": y + h / 2.0,
            }
        )

    tokens.sort(key=lambda t: (t["yc"], t["x"]))
    rows = []
    current_row = []
    last_y = None
    row_threshold = 10

    for t in tokens:
        if last_y is None or abs(t["yc"] - last_y) <= row_threshold:
            current_row.append(t)
        else:
            if current_row:
                current_row.sort(key=lambda z: z["x"])
                row_text = " ".join(tok["text"] for tok in current_row)
                rows.append({"text": row_text, "tokens": current_row})
            current_row = [t]
        last_y = t["yc"]

    if current_row:
        current_row.sort(key=lambda z: z["x"])
        row_text = " ".join(tok["text"] for tok in current_row)
        rows.append({"text": row_text, "tokens": current_row})

    return lines, notes, rows



def _process_page_optimized(img): # replaces _ocr_page_to_lines_and_grid
    """
    Runs OCR once per page using image_to_data.
    Reconstructs lines from the data to avoid calling image_to_string separately.
    """
    config = f"--psm {OPTIMAL_PSM}"
    
    # 1. Single OCR Pass: Get Data (includes coordinates and text)
    try:
        data = pytesseract.image_to_data(
            img, lang="deu+eng", config=config, output_type=Output.DICT
        )
    except Exception as e:
        logging.error(f"OCR failed on page: {e}")
        return [], [], []

    # 2. Reconstruct Logic
    # We filter empty tokens and build our own structures
    n = len(data["text"])
    tokens = []
    lines_reconstructed = []
    
    # Group by line number (Tesseract provides 'line_num' in data dict)
    # However, Tesseract's line_num can be flaky in tables, so we use your 
    # existing coordinate sorting logic which is more robust.
    
    for i in range(n):
        txt = data["text"][i]
        if not txt or txt.isspace():
            continue
            
        x, y, w, h = data["left"][i], data["top"][i], data["width"][i], data["height"][i]
        tokens.append({
            "text": txt,
            "x": x, "y": y, "w": w, "h": h,
            "xc": x + w / 2.0,
            "yc": y + h / 2.0,
        })

    # Sort tokens by vertical position (y), then horizontal (x)
    tokens.sort(key=lambda t: (t["yc"], t["x"]))

    # 3. Grid/Line Construction (Reusing your existing logic pattern)
    rows = []
    current_row = []
    last_y = None
    row_threshold = 10  # pixels

    for t in tokens:
        if last_y is None or abs(t["yc"] - last_y) <= row_threshold:
            current_row.append(t)
        else:
            if current_row:
                current_row.sort(key=lambda z: z["x"])
                row_text = " ".join(tok["text"] for tok in current_row)
                rows.append({"text": row_text, "tokens": current_row})
                lines_reconstructed.append(row_text)
            current_row = [t]
        last_y = t["yc"]

    if current_row:
        current_row.sort(key=lambda z: z["x"])
        row_text = " ".join(tok["text"] for tok in current_row)
        rows.append({"text": row_text, "tokens": current_row})
        lines_reconstructed.append(row_text)
        
    # Extract notes from the reconstructed text
    full_text_blob = "\n".join(lines_reconstructed)
    notes = NOTE_RE.findall(full_text_blob)

    return lines_reconstructed, notes, rows


def detect_ects_column(rows):

    header_keywords = ["ects", "lp", "credit",
                       "credits", "leistungspunkte", "cp"]

    candidates = []

    for row in rows[:20]:
        for tok in row["tokens"]:
            txt = tok["text"].lower()
            for kw in header_keywords:
                if kw in txt:
                    candidates.append(tok["xc"])
                    break

    if not candidates:
        return None

    candidates.sort()
    mid = len(candidates) // 2
    if len(candidates) % 2 == 1:
        ects_x = candidates[mid]
    else:
        ects_x = (candidates[mid - 1] + candidates[mid]) / 2.0
    return ects_x


def extract_ects_from_row(row, ects_x, max_distance=40):

    best_token = None
    best_dist = None

    for tok in row["tokens"]:
        txt = tok["text"].replace(",", ".")
        if not NUM_RE.match(txt):
            continue
        try:
            val = float(txt)
        except ValueError:
            continue
        if val <= 0 or val > 40:
            continue
        dist = abs(tok["xc"] - ects_x)
        if dist <= max_distance and (best_dist is None or dist < best_dist):
            best_token = val
            best_dist = dist

    return best_token


def _resolve_conflicts_keep_specific(mods):

    if len(mods) <= 1:
        return mods

    temp = [(m, normalize_text(m.name)) for m in mods]
    temp.sort(key=lambda x: len(x[1]), reverse=True)

    kept = []
    kept_norms = []
    for m, norm in temp:
        if any(norm in kn for kn in kept_norms if norm != kn):
            continue
        kept.append(m)
        kept_norms.append(norm)

    return kept


def match_modules_in_row(row_text, module_map, allow_fuzzy=True):

    text_norm = normalize_text(row_text)
    if not text_norm:
        return []

    strict_hits = []
    fuzzy_hits = []

    for mod in module_map:
        mod_norm = normalize_text(mod.name)
        if not mod_norm:
            continue

        if mod_norm in text_norm:
            strict_hits.append(mod)
        elif allow_fuzzy:
            score = SequenceMatcher(None, mod_norm, text_norm).ratio()
            if score >= FUZZY_THRESHOLD:
                fuzzy_hits.append(mod)

    if strict_hits:
        return _resolve_conflicts_keep_specific(strict_hits)
    if fuzzy_hits:
        return _resolve_conflicts_keep_specific(fuzzy_hits)
    return []


def _build_module_list_from_mapping(module_map_dict):
    return [Module(
                name=str(name).strip().lower(),
                category=str(cat).strip(),
                ects=None)
            for name, cat in module_map_dict.items()
            if name is not None and cat is not None]


def extract_ects_ocr(pdf_path, module_map_dict, categories):
    modules = _build_module_list_from_mapping(module_map_dict)
    
    # 1. Quick Fail Checks
    if not modules or not os.path.exists(pdf_path):
        return {cat: 0.0 for cat in categories}, [], [], "ocr_skipped"

    # 2. Image Conversion (Single Pass at 300 DPI)
    # 300 DPI is the industry standard for OCR. 100 is too low for small footnote text in transcripts.
    try:
        kwargs = {"dpi": OPTIMAL_DPI}
        if POPPLER_PATH:
            kwargs["poppler_path"] = POPPLER_PATH
        
        # We process the whole PDF. If it's huge, you might want to limit 'last_page'
        pages = convert_from_path(pdf_path, **kwargs)
    except Exception as e:
        logging.error(f"PDF Rasterization failed {pdf_path}: {e}")
        return {cat: 0.0 for cat in categories}, [], [], "rasterization_failed"

    # 3. Parallel OCR Processing
    # We parallelize by PAGE here.
    
    line_matches = []
    col_matches = []
    ects_per_cat_line = {cat: 0.0 for cat in categories}
    ects_per_cat_col = {cat: 0.0 for cat in categories}
    
    # Accumulate data from all pages
    with ThreadPoolExecutor(max_workers=OCR_THREADS) as executor:
        results = executor.map(_process_page_optimized, pages)

    # 4. Aggregation Logic
    # We iterate through the results of the pages
    line_seen = set()
    col_seen = set()

    for lines, notes, rows in results:
        
        # A. Detect ECTS Column for this page
        ects_x = detect_ects_column(rows)

        # B. Strategy 1: Line-based matching
        for ln in lines:
            if is_trash_line(ln): continue
            
            found_mods = match_modules_in_row(ln, modules, allow_fuzzy=True)
            for m in found_mods:
                if m.category not in categories: continue
                
                ects_val = m.ects if m.ects is not None else 0.0
                # Unique key: (Module Name, Category, ECTS) to avoid double counting same line
                key = (m.name, m.category, ects_val)
                
                if key not in line_seen:
                    line_seen.add(key)
                    ects_per_cat_line[m.category] += ects_val
                    line_matches.append((m.name, m.category, ects_val, ln))

        # C. Strategy 2: Column-based extraction (if column detected)
        if ects_x is not None:
            for row in rows:
                found_mods = match_modules_in_row(row["text"], modules, allow_fuzzy=True)
                if not found_mods: continue
                
                # Look for number at specific X position
                ects_val = extract_ects_from_row(row, ects_x)
                if ects_val is None: continue

                for m in found_mods:
                    if m.category not in categories: continue
                    
                    key = (m.name, m.category, ects_val)
                    if key not in col_seen:
                        col_seen.add(key)
                        ects_per_cat_col[m.category] += ects_val
                        col_matches.append((m.name, m.category, ects_val, row["text"]))

    # 5. Final Selection (Best Strategy)
    total_line = sum(ects_per_cat_line.values())
    total_col = sum(ects_per_cat_col.values())

    # We prefer column extraction if it yields results, as it validates the number position
    # But if column extraction failed (no header found), we fall back to line matching
    if total_col > 0:
        best_sums = ects_per_cat_col
        best_matches = [f"{n} -> {c}:{e} | {txt}" for (n, c, e, txt) in col_matches]
        best_method = "ocr_optimized_column"
    else:
        best_sums = ects_per_cat_line
        best_matches = [f"{n} -> {c}:{e} | {txt}" for (n, c, e, txt) in line_matches]
        best_method = "ocr_optimized_line"

    best_sums = {k: round(float(v), 2) for k, v in best_sums.items()}
    return best_sums, best_matches, [], best_method
