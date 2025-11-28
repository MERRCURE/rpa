import re
import logging

from utils.claimed_dom_extract import _floatcast

try:
    import pdfplumber
except Exception:
    pdfplumber = None


def _norm_space(text: str) -> str:
    return " ".join(str(text).split()) if text else ""

_NAME_PATTERN = re.compile(r"[^0-9a-zäöüß ]+")
def _norm_name(text: str) -> str:
    if text is None:
        return ""
    t = text.lower()
    t = _NAME_PATTERN.sub(" ", t)
    t = " ".join(t.split())
    return t

# Group 1 (Prefix): Starts with alphanumeric/symbols followed by space (e.g., "CS-101 ")
# Group 2 (Suffix): Optional hyphen/space, number, "CP", and everything after (e.g., " - 5 CP ...")
_CLEANUP_PATTERN = re.compile(
    r"(?:^[a-z0-9\-/.]+\s+)|(?:[\s-]*\d+(?:[.,]\d+)?\s*cp\b.*)", 
    re.IGNORECASE
)
def _strip_module_code(cell: str) -> str:
    if not cell:
        return ""
    s = _norm_space(cell)
    s = _CLEANUP_PATTERN.sub("", s)
    return s.strip(" -")

ECTS_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*CP", re.IGNORECASE)

def extract_tables_from_pdf(pdf_path: str):

    if pdfplumber is None:
        logging.warn("pdfplumber nicht installiert, kann Tabellen nicht lesen.")
        return []

    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            try:

                tables = page.extract_tables(
                    {
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                        "intersection_tolerance": 5,
                    }
                )
            except Exception as e:
                logging.warn(
                    f"pdfplumber-Fehler auf Seite {page_idx+1}: {e}")
                continue

            if not tables:
                continue

            for table in tables:
                for row in table:
                    if not row:
                        continue
                    cells = [_norm_space(c) for c in row]
                    if not any(c.strip() for c in cells):
                        continue
                    rows.append(cells)

    logging.debug(
        f"pdf_table_extract: {len(rows)} Zeilen in {pdf_path} gefunden.")
    return rows

SKIP_ROW_RE = re.compile(
    r"(?=.*fach)(?=.*ects)|transcript of records|^\s*gesamt\s*$", 
    re.IGNORECASE
)
EXTRACT_GRADES_PATTERN = re.compile(r"\b\d+(?:[.,]\d+)?\b")
def parse_modules_from_rows(rows):

    modules = []

    for row in rows:
        cells = [_norm_space(c) for c in row]
        if not any(cells):
            continue
        
        joined = " ".join(cells).strip()
        if SKIP_ROW_RE.search(joined):
            continue

        name_cell = ""
        for c in cells:
            if c:
                name_cell = c
                break

        clean_name = _strip_module_code(name_cell)
        if not clean_name:
            clean_name = name_cell

        ects_val = None
        for c in cells:
            m = ECTS_RE.search(c)
            if m:
                try:
                    v = _floatcast(m.group(1))
                    if 0.0 < v <= 50.0:
                        ects_val = v
                        break
                except ValueError:
                    continue

        if ects_val is None:
            for c in cells:
                for m in EXTRACT_GRADES_PATTERN.finditer(c):
                    try:
                        v = _floatcast(m.group(0))
                        if 0.0 < v <= 50.0:
                            ects_val = v
                            break
                    except ValueError:
                        continue
                if ects_val is not None:
                    break

        if ects_val is None:
            continue

        modules.append(
            {
                "raw_name": name_cell,
                "name": clean_name,
                "ects": ects_val,
            }
        )

    logging.debug(f"pdf_table_extract: {len(modules)} Modulzeilen erkannt.")
    return modules


RE_ROMAN = re.compile(r"\b[ivx]+\b")

def sum_ects_by_category(modules, module_map, categories):

    def norm_name(s: str) -> str:
        return _norm_name(s)

    def strip_roman(s: str) -> str:
        return RE_ROMAN.sub("", s).strip()

    sums = {cat: 0.0 for cat in categories}
    matched_modules = []
    similar_matches = []

    csv_norm = {norm_name(name): cat for name, cat in module_map.items()}

    for mod in modules:
        raw_name = mod["name"]
        ects = float(mod["ects"])
        norm_mod = norm_name(raw_name)

        if not norm_mod:
            continue

        if norm_mod in csv_norm:
            cat = csv_norm[norm_mod]
            if cat in categories:
                sums[cat] += ects
                matched_modules.append(
                    f"{mod['raw_name']} -> {cat}:{ects}"
                )
            continue

        base_mod = strip_roman(norm_mod)
        for csv_name_norm, cat in csv_norm.items():
            if cat not in categories:
                continue
            base_csv = strip_roman(csv_name_norm)
            if base_mod and base_mod == base_csv and base_mod != norm_mod:
                similar_matches.append(
                    f"{mod['raw_name']} ~ {csv_name_norm}"
                )
                break

    sums = {k: round(v, 2) for k, v in sums.items()}
    return sums, matched_modules, similar_matches
