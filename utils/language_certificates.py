#!/usr/bin/env python3
import re
import logging
from typing import List, Tuple, Optional

from .ocr_ects import ocr_text_from_pdf
from .grading_systems import normalize_country_name
from utils.claimed_dom_extract import _floatcast

def _merge_texts(pdf_paths: List[str]) -> str:
    parts = []
    for p in pdf_paths:
        try:
            parts.append(ocr_text_from_pdf(p))
        except Exception as e:
            logging.error(
                f"language certicate couldnt be read{p}: {e}")
    return "\n".join(parts)


GERMAN_C1_C2_RE = re.compile(
    r"""
    dsh[-\s]?[23]|                          # Matches DSH-2 or DSH-3
    testdaf|
    goethe[-\s]?zertifikat\s*c2|
    (?:kleine|große)[s]?\s+deutsche[s]?\s+sprachdiplom|  # Combined Small/Big
    zentrale\s+oberstufenpr[üu]fung|        # [üu] is faster than (ü|u)
    deutsches\s+sprachdiplom|
    telc\s+deutsch\s+c1\s+hochschule|
    \b(?:ö|oe)sd\s*c2|                      # (?:...) is non-capturing (faster)
    (?:österreichisches|oesterreichisches)\s+sprachdiplom
    """,
    re.IGNORECASE | re.VERBOSE
)

def evaluate_language_status_bwl(
    lang_pdfs: List[str],
    bachelor_country_raw: str,
) -> str:

    norm_country = normalize_country_name(bachelor_country_raw or "")
    if norm_country == "germany":
        return "not necessary"

    if not lang_pdfs:
        return "required but not available"

    text = _merge_texts(lang_pdfs)

    if GERMAN_C1_C2_RE.search(text):
        return "available (German C1/C2 or equivalent)"
    else:
        return "available (unclassified German certificate)"

TOEFL_PATTERN = re.compile(r"\b\d{2,3}\b")
IELTS_PATTERN = re.compile(r"\b\d(?:[.,]\d)?\b")

def evaluate_language_status_ai(
    lang_pdfs: List[str],
) -> str:
    """
    AI: English-taught. Everybody needs English proof, but we only report status.
    """
    if not lang_pdfs:
        return "required but not available"

    text = _merge_texts(lang_pdfs).lower()

    # TOEFL
    if "toefl" in text:
        nums = [_floatcast(x)
                for x in TOEFL_PATTERN.findall(text)]
        if any(n >= 500 for n in nums) or any(n >= 200 for n in nums) or any(n >= 80 for n in nums):
            return "available (likely sufficient TOEFL)"
        else:
            return "available (TOEFL but unclear score)"

    # IELTS
    if "ielts" in text:
        nums = [_floatcast(x)
                for x in IELTS_PATTERN.findall(text)]
        if any(n >= 6.0 for n in nums):
            return "available (likely sufficient IELTS)"
        else:
            return "available (IELTS but unclear score)"

    # Cambridge / Linguaskill
    if "cambridge" in text or "linguaskill" in text:
        if "b2" in text or "c1" in text:
            return "available (Cambridge/Linguaskill B2+)"
        else:
            return "available (Cambridge/Linguaskill unclassified)"

    #  Abitur
    if "abitur" in text and "engl" in text:
        return "available (German Abitur with English)"

    # Medium of instruction
    if "medium of instruction" in text or "language of instruction" in text:
        if "english" in text:
            return "available (degree taught in English)"
        else:
            return "available (medium of instruction unclassified)"

    return "available (unclassified English certificate)"
