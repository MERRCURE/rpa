#!/usr/bin/env python3
import os
import re
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher

from pdf2image import convert_from_path
import pytesseract
from pytesseract import Output

import os
import platform
import pytesseract

POPPLER_PATH = os.environ.get("POPPLER_PATH", None)

tess_env_cmd = os.environ.get("TESSERACT_CMD")


def configure_tesseract():
    env_cmd = os.environ.get("TESSERACT_CMD")

    if env_cmd and os.path.exists(env_cmd):
        pytesseract.pytesseract.tesseract_cmd = env_cmd
        print(f"INFO: Using TESSERACT_CMD={env_cmd}")
        return

    system = platform.system()

    if system == "Linux":
        found = shutil.which("tesseract")
        if found:
            pytesseract.pytesseract.tesseract_cmd = found
            print(f"INFO: Found Tesseract on Linux at {found}")
            return

    if system == "Darwin":
        possible = [
            "/usr/local/bin/tesseract",
            "/opt/homebrew/bin/tesseract",
        ]
        for p in possible:
            if os.path.exists(p):
                pytesseract.pytesseract.tesseract_cmd = p
                print(f"INFO: Found Tesseract on macOS at {p}")
                return
        # fallback: try PATH
        found = shutil.which("tesseract")
        if found:
            pytesseract.pytesseract.tesseract_cmd = found
            print(f"INFO: Found Tesseract via PATH at {found}")
            return

    if system == "Windows":
        possible = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
        for p in possible:
            if os.path.exists(p):
                pytesseract.pytesseract.tesseract_cmd = p
                print(f"INFO: Found Tesseract on Windows at {p}")
                return

        found = shutil.which("tesseract")
        if found:
            pytesseract.pytesseract.tesseract_cmd = found
            print(f"INFO: Found Tesseract on Windows via PATH at {found}")
            return

    raise RuntimeError(
        "Tesseract not found. Please install Tesseract or set TESSERACT_CMD."
    )


CPU_THREADS = os.cpu_count() or 1
OCR_THREADS = max(1, int(CPU_THREADS * 0.9))

print(
    f"INFO: OCR engine detected {CPU_THREADS} CPU threads, "
    f"using up to {OCR_THREADS} threads for page-level OCR."
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

def is_trash_line(line: str) -> bool:

    if not line.strip():
        return True

    if re.search(r"[A-Za-zÄÖÜäöüß]{4,}", line):
        return False

    if re.search(r"\d", line):
        return True

    return False


NOTE_RE = re.compile(r"\b([0-6][.,]\d+)\b")
NUM_RE = re.compile(r"^\d+(?:[.,]\d+)?$")

Module = namedtuple("Module", ["name", "category", "ects"])
FUZZY_THRESHOLD = 0.80


def _ocr_page_to_lines_and_grid(args):

    img, dpi, psm = args
    config = f"--psm {psm}"

    text = pytesseract.image_to_string(img, lang="deu+eng", config=config)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    notes = NOTE_RE.findall(text)

    data = pytesseract.image_to_data(
        img, lang="deu+eng", config=config, output_type=Output.DICT
    )

    n = len(data["text"])
    tokens = []
    for i in range(n):
        txt = data["text"][i]
        if not txt or txt.strip() == "" or txt.strip() == "|":
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

    modules = []
    for name, cat in module_map_dict.items():
        if not name or not cat:
            continue
        modules.append(
            Module(
                name=str(name).strip().lower(),
                category=str(cat).strip(),
                ects=None,
            )
        )
    return modules


def extract_ects_ocr(pdf_path, module_map_dict, categories):

    modules = _build_module_list_from_mapping(module_map_dict)
    if not modules:
        return {cat: 0.0 for cat in categories}, [], [], "ocr_hocr"

    if not os.path.exists(pdf_path):
        print(f"WARNING: extract_ects_ocr() with non-existing PDF: {pdf_path}")
        return {cat: 0.0 for cat in categories}, [], [], "ocr_hocr"

    DPIS = [200, 300]
    PSM_VALUES = [4, 6]

    best_total_ects = -1.0
    best_sums = {cat: 0.0 for cat in categories}
    best_matches = []
    best_method = "ocr_hocr"

    for dpi in DPIS:
        kwargs = {"dpi": dpi}
        if POPPLER_PATH:

            kwargs["poppler_path"] = POPPLER_PATH

        pages = convert_from_path(pdf_path, **kwargs)

        for psm in PSM_VALUES:
            args = [(img, dpi, psm) for img in pages]

            line_matches = []
            col_matches = []
            ects_per_cat_line = {cat: 0.0 for cat in categories}
            ects_per_cat_col = {cat: 0.0 for cat in categories}

            notes_detected = []

            line_seen = set()
            col_seen = set()

            with ThreadPoolExecutor(max_workers=OCR_THREADS) as executor:
                for lines, notes, rows in executor.map(_ocr_page_to_lines_and_grid, args):
                    notes_detected.extend(notes)

                    ects_x = detect_ects_column(rows)

                    for ln in lines:
                        if is_trash_line(ln):
                            continue
                        mods = match_modules_in_row(
                            ln, modules, allow_fuzzy=True)
                        for m in mods:
                            ects_val = m.ects if m.ects is not None else 0.0
                            if m.category not in categories:
                                continue
                            key = (m.name, m.category, ects_val)
                            if key in line_seen:
                                continue
                            line_seen.add(key)
                            ects_per_cat_line[m.category] += ects_val
                            line_matches.append(
                                (m.name, m.category, ects_val, ln)
                            )

                    if ects_x is not None:
                        for row in rows:
                            mods = match_modules_in_row(
                                row["text"], modules, allow_fuzzy=True
                            )
                            if not mods:
                                continue
                            ects_val = extract_ects_from_row(row, ects_x)
                            if ects_val is None:
                                continue
                            for m in mods:
                                if m.category not in categories:
                                    continue
                                key = (m.name, m.category, ects_val)
                                if key in col_seen:
                                    continue
                                col_seen.add(key)
                                ects_per_cat_col[m.category] += ects_val
                                col_matches.append(
                                    (m.name, m.category, ects_val, row["text"])
                                )

            total_line = sum(ects_per_cat_line.values())
            total_col = sum(ects_per_cat_col.values())
            effective_total = total_col if col_matches else total_line

            if effective_total > best_total_ects:
                best_total_ects = effective_total
                if col_matches:
                    best_sums = ects_per_cat_col
                    best_matches = [
                        f"{n} -> {c}:{e} | {txt}"
                        for (n, c, e, txt) in col_matches
                    ]
                    best_method = "ocr_hocr_column"
                else:
                    best_sums = ects_per_cat_line
                    best_matches = [
                        f"{n} -> {c}:{e} | {txt}"
                        for (n, c, e, txt) in line_matches
                    ]
                    best_method = "ocr_hocr_line"

    best_sums = {k: round(float(v), 2) for k, v in best_sums.items()}
    unrecognized_lines = []
    return best_sums, best_matches, unrecognized_lines, best_method
