#!/usr/bin/env python3
import os
import re
import logging
from typing import Dict, List, Tuple

# Import the centralized OCR logic
from .ocr_ects import ocr_text_from_pdf
from .ocr_engine import normalize_text

TRANSCRIPT_KEYWORDS = [
    "transcript of records", "transcript of academic record", "grade report",
    "leistungsübersicht", "notenübersicht", "notenspiegel", "leistungsnachweis",
    "official transcript", "academic transcript", "student transcript",
    "unofficial transcript", "university transcript", "course transcript",
    "academic record", "record of study", "record of academic work",
    "course history", "study history", "study record", "marksheet",
    "mark sheet", "marks sheet", "statement of marks", "statement of results",
    "grade history", "performance report", "performance transcript",
]
ECTS_KEYWORDS = ["ects", "leistungspunkte", "credits", "credit points", "cp "]
SEMESTER_RE = re.compile(
    r"(wise|sose|wintersemester|sommersemester|ws ?20|ss ?20)")
LINE_WITH_DIGIT_RE = re.compile(r"^.*\d.*$", re.MULTILINE)


def score_transcript(text_low: str, text_norm: str) -> int:
    score = 0

    if any(kw in text_low for kw in TRANSCRIPT_KEYWORDS):
        score += 4

    if any(kw in text_low for kw in ECTS_KEYWORDS):
        score += 3

    if len(SEMESTER_RE.findall(text_low)) >= 2:
        score += 2

    # Heuristic: Transcripts usually have many lines with numbers (grades/credits)
    numeric_line_count = len(LINE_WITH_DIGIT_RE.findall(text_low))

    if numeric_line_count > 20:
        score += 1

    return score


GERMAN_CERT_KEYWORDS = [
    "dsh-2", "dsh-3", "testdaf", "goethe-zertifikat c2",
    "zentrale oberstufenpruefung", "zentrale oberstufenprüfung",
    "deutsches sprachdiplom", "telc deutsch c1 hochschule",
    "österreichisches sprachdiplom", "oesd c2",
    "österreichische sprachdiplom c2",
]

# Generic terms that indicate a language exam took place, but aren't specific certificates
GERMAN_GENERIC_KEYWORDS = ("sprachprüfung", "language exam")
ENGLISH_CERT_KEYWORDS = [
    "toefl", "test of english as a foreign language", "ielts",
    "cambridge english", "b2 first", "first certificate",
    "linguaskill", "language test report form", "english language test",
]
ENGLISH_GENERIC_KEYWORDS = ("overall band", "overall score")


def score_language_cert(text_low: str, program: str) -> int:
    score = 0
    prog = program.lower()

    if prog == "bwl":
        if any(kw in text_low for kw in GERMAN_CERT_KEYWORDS):
            score += 5
        if any(kw in text_low for kw in GERMAN_GENERIC_KEYWORDS):
            score += 2

    elif prog == "ai":
        if any(kw in text_low for kw in ENGLISH_CERT_KEYWORDS):
            score += 5
        if any(kw in text_low for kw in ENGLISH_GENERIC_KEYWORDS):
            score += 2

    return score


MODULE_OVERVIEW_KEYWORDS = [

    "module overview",
    "modulübersicht",
    "moduluebersicht",
    "curriculum",
    "course catalogue",
    "module catalogue",
    "study plan",
    "modulkatalog",
]

GRADE_WORDS = ["note", "grade", "bewertung", "ergebnis", "result"]


def score_module_overview(text_low: str) -> int:
    score = 0

    if any(kw in text_low for kw in MODULE_OVERVIEW_KEYWORDS):
        score += 5

    ects_count = text_low.count("ects") + text_low.count("lp")
    if ects_count >= 5:
        score += 3

    # Should not  contain many grade keywords hopefully distingushed from transcipt relieable
    if sum(text_low.count(gw) for gw in GRADE_WORDS) < 3:
        score += 2

    return score


DEGREE_KEYWORDS = [
    "bachelorzeugnis", "zeugnis", "urkunde", "bachelor of science",
    "bachelor of arts", "bachelor of engineering", "bachelor of",
    "degree certificate", "degree", "diploma", "baccalaureate",
    "this is to certify that", "has been awarded the degree",
]
DEGREE_GRADE_KEYWORDS = ["gesamtnote", "abschlussnote", "overall grade"]
TRANSCRIPT_INDICATORS = ("transcript", "ects", "credits")


def score_degree_certificate(text_low: str, text_norm: str) -> int:
    score = 0

    if any(kw in text_low for kw in DEGREE_KEYWORDS):
        score += 4
    if any(kw in text_low for kw in DEGREE_GRADE_KEYWORDS):
        score += 2

    # Negative check: Ensure it doesn't look like a Transcript
    # If NONE of the transcript indicators are present, add a point.
    if not any(kw in text_low for kw in TRANSCRIPT_INDICATORS):
        score += 1

    return score


VPD_KEYWORDS = [
    "vorprüfungsdokumentation",
    "vorpruefungsdokumentation",
    "vpd",
    "uni-assist",
    "uni assist"
]
# These must ALL be present to trigger the bonus score
VPD_CONTENT_PHRASES = ("bewertung", "ausländischer hochschulabschluss")


def score_vpd(text_low: str) -> int:
    score = 0

    # 1. Check strong VPD keywords (OR logic)
    if any(kw in text_low for kw in VPD_KEYWORDS):
        score += 6

    # 2. Check for specific phrase combination (AND logic)
    if all(phrase in text_low for phrase in VPD_CONTENT_PHRASES):
        score += 2

    return score


def classify_document(pdf_path: str, program: str) -> Tuple[str, Dict[str, int]]:
    logging.info(f"Classifying: {os.path.basename(pdf_path)}")

    # -------------------------------------------------------------
    # OPTIMIZATION: Only OCR the first page for classification
    # -------------------------------------------------------------
    text = ocr_text_from_pdf(pdf_path, max_pages=1)

    if not text.strip():
        return "other", {"transcript": 0, "language_certificate": 0, "degree_certificate": 0, "vpd": 0}

    text_low = text.lower()
    text_norm = normalize_text(text)

    scores = {
        "module_overview": score_module_overview(text_low),
        "transcript": score_transcript(text_low, text_norm),
        "language_certificate": score_language_cert(text_low, program),
        "degree_certificate": score_degree_certificate(text_low, text_norm),
        "vpd": score_vpd(text_low),
    }

    best_type = max(scores, key=scores.get)
    best_score = scores[best_type]

    # Threshold: If the best match is weak, call it 'other'
    doc_type = best_type if best_score >= 2 else "other"

    return doc_type, scores


def classify_many(pdf_paths: List[str], program: str):
    by_type = {
        "module_overview": [],
        "transcript": [],
        "language_certificate": [],
        "degree_certificate": [],
        "vpd": [],
        "other": [],
    }

    best_transcript = (None, None)
    best_transcript_score = -1

    for pdf_path in pdf_paths:
        doc_type, scores = classify_document(pdf_path, program)
        by_type.setdefault(doc_type, []).append(pdf_path)

        # Track the 'strongest' transcript candidate
        if doc_type == "transcript":
            sc = scores.get("transcript", 0)
            if sc > best_transcript_score:
                best_transcript_score = sc
                best_transcript = (pdf_path, scores)

    return {
        "by_type": by_type,
        "best_transcript": best_transcript,
    }
