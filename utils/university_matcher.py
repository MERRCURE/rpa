import re
import unicodedata

UNI_KEYWORDS = (
    "hochschule",
    "fachhochschule",
    "universität",
    "university",
    "college",
    "academy",
    "akademie",
    "institut",
    "institute",
    "polytechnic",
    "school of",
)

def _norm(s):
    if not s:
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("ß","ss")
    s = re.sub(r"[^a-z0-9 ]"," ", s)
    s = re.sub(r"\s+"," ", s).strip()
    return s

def extract_university_candidates(text):
    cands = []
    for ln in text.splitlines():
        low = ln.lower()
        if any(k in low for k in UNI_KEYWORDS):
            cleaned = _norm(ln)
            if len(cleaned) >= 5:
                cands.append(cleaned)
    return cands

def is_whitelisted_university_in_pdf(pdf_paths, whitelist_set, ocr_func):
    norm_whitelist = [_norm(w) for w in whitelist_set]

    for pdf in pdf_paths:
        txt = ocr_func(pdf, max_pages=1)
        if not txt.strip():
            continue

        cands = extract_university_candidates(txt)

        for cand in cands:
            if cand in norm_whitelist:
                return True, cand

    return False, None