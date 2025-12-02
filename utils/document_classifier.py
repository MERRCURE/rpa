#!/usr/bin/env python3
import os
import re
import logging
from typing import Dict, List, Tuple
from .ocr_ects import ocr_text_from_pdf
from .ocr_engine import normalize_text

TRANSCRIPT_KEYWORDS = [
    "transcript of records", "transcript of academic record", "grade report",
    "leistungsübersicht", "notenübersicht", "notenspiegel", "leistungsnachweis",
    "academic transcript", "student transcript", "official transcript",
    "study record", "course history", "marksheet", "mark sheet",
    "statement of marks", "statement of results"
]

ECTS_KEYWORDS = ["ects", "leistungspunkte", "credits", "credit points", "cp "]

SEMESTER_RE = re.compile(r"(wise|sose|wintersemester|sommersemester|ws ?20|ss ?20)")
LINE_WITH_DIGIT_RE = re.compile(r".*\d.*")

GERMAN_CERT_KEYWORDS = [
    "dsh-2", "dsh-3", "testdaf", "goethe-zertifikat", "deutsches sprachdiplom",
    "telc deutsch", "ösd", "ösd", "sprachprüfung"
]

ENGLISH_CERT_KEYWORDS = [
    "toefl", "ielts", "cambridge english", "linguaskill",
    "first certificate", "language test report form"
]

MODULE_OVERVIEW_KEYWORDS = [
    "module overview", "modulübersicht", "moduluebersicht",
    "module catalogue", "course catalogue", "study plan", "modulkatalog",
    "curriculum"
]

GRADE_WORDS = ["note", "grade", "bewertung", "ergebnis", "result"]

DEGREE_KEYWORDS = [
    "bachelorzeugnis", "zeugnis", "urkunde", "bachelor of science",
    "bachelor of arts", "bachelor of engineering", "degree certificate",
    "diploma", "this is to certify that", "has been awarded the degree"
]

TRANSCRIPT_INDICATORS = ("transcript", "ects", "credits")

VPD_KEYWORDS = ["vorprüfungsdokumentation", "vorpruefungsdokumentation", "uni-assist", "vpd"]
VPD_PHRASES = ("bewertung", "ausländischer hochschulabschluss")


def score_transcript(text_low, text_norm):
    score = 0
    if any(k in text_low for k in TRANSCRIPT_KEYWORDS):
        score += 5
    if any(k in text_low for k in ECTS_KEYWORDS):
        score += 3
    if len(SEMESTER_RE.findall(text_low)) >= 1:
        score += 2
    numeric_count = sum(1 for ln in text_low.splitlines() if LINE_WITH_DIGIT_RE.search(ln))
    if numeric_count >= 15:
        score += 2
    return score


def score_language_cert(text_low, program):
    score = 0
    if program == "bwl":
        if any(k in text_low for k in GERMAN_CERT_KEYWORDS):
            score += 5
    else:
        if any(k in text_low for k in ENGLISH_CERT_KEYWORDS):
            score += 5
    return score


def score_module_overview(text_low):
    score = 0
    if any(k in text_low for k in MODULE_OVERVIEW_KEYWORDS):
        score += 6
    ects_count = text_low.count("ects") + text_low.count("lp")
    if ects_count >= 4:
        score += 3
    grade_hits = sum(text_low.count(g) for g in GRADE_WORDS)
    if grade_hits <= 2:
        score += 1
    return score


def score_degree_certificate(text_low, text_norm):
    score = 0
    if any(k in text_low for k in DEGREE_KEYWORDS):
        score += 4
    grade_hits = sum(text_low.count(w) for w in GRADE_WORDS)
    if grade_hits >= 1:
        score += 1
    if not any(k in text_low for k in TRANSCRIPT_INDICATORS):
        score += 1
    return score


def score_vpd(text_low):
    score = 0
    if any(k in text_low for k in VPD_KEYWORDS):
        score += 7
    if all(p in text_low for p in VPD_PHRASES):
        score += 3
    return score


def classify_document(pdf_path, program):
    logging.info(f"Classifying: {os.path.basename(pdf_path)}")

    text = ocr_text_from_pdf(pdf_path, max_pages=1)
    if not text.strip():
        return "other", {}

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

    if best_score < 3:
        return "other", scores

    return best_type, scores


def classify_many(pdf_paths, program):
    by_type = {
        "module_overview": [],
        "transcript": [],
        "language_certificate": [],
        "degree_certificate": [],
        "vpd": [],
        "other": [],
    }

    best_transcript = (None, None)
    best_score = -1

    for p in pdf_paths:
        doc_type, scores = classify_document(p, program)
        by_type[doc_type].append(p)

        if doc_type == "transcript":
            sc = scores.get("transcript", 0)
            if sc > best_score:
                best_score = sc
                best_transcript = (p, scores)

    return {"by_type": by_type, "best_transcript": best_transcript}