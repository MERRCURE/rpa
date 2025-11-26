#!/usr/bin/env python3
import os
import re

from pdf2image import convert_from_path
import pytesseract

from .ocr_engine import normalize_text

def quick_ocr_preview(pdf_path: str, dpi: int = 100, psm: int = 6, max_pages: int = 2) -> str:
    print("--> Here, secretly also doing some extra OCR <--")
    print(f"   {pdf_path}")
    pages = convert_from_path(pdf_path, dpi=dpi, last_page=max_pages)
    text_parts = [
        pytesseract.image_to_string(img, lang="deu+eng", config=f"--psm {psm}")
        for img in pages
        ]
    return "\n".join(text_parts)


def score_transcript(text_low: str, text_norm: str) -> int:
    score = 0

    keywords_strong = [
        "transcript of records",
        "transcript of academic record",
        "grade report",
        "leistungsübersicht",
        "notenübersicht",
        "notenspiegel",
        "leistungsnachweis",
        "official transcript",
        "academic transcript",
        "student transcript",
        "unofficial transcript",
        "university transcript",
        "course transcript",
        "academic record",
        "record of study",
        "record of academic work",
        "course history",
        "study history",
        "study record",
        "marksheet",
        "mark sheet",
        "marks sheet",
        "statement of marks",
        "statement of results",
        "grade history",
        "performance report",
        "performance transcript",
    ]
    if any(kw in text_low for kw in keywords_strong):
        score += 4

    if any(kw in text_low for kw in ["ects", "leistungspunkte", "credits", "credit points", "cp "]):
        score += 3

    if len(re.findall(r"(wise|sose|wintersemester|sommersemester|ws ?20|ss ?20)", text_low)) >= 2:
        score += 2

    if len(re.findall(r"\n.*\d+.*\n", text_low)) > 20:
        score += 1

    return score


def score_language_cert(text_low: str, program: str) -> int:
    score = 0
    prog = program.lower()

    if prog == "bwl":

        de_certs = [
            "dsh-2",
            "dsh-3",
            "testdaf",
            "goethe-zertifikat c2",
            "zentrale oberstufenpruefung",
            "zentrale oberstufenprüfung",
            "deutsches sprachdiplom",
            "telc deutsch c1 hochschule",
            "österreichisches sprachdiplom",
            "oesd c2",
            "österreichische sprachdiplom c2",
        ]
        if any(kw in text_low for kw in de_certs):
            score += 5

        if "sprachprüfung" in text_low or "language exam" in text_low:
            score += 2

    elif prog == "ai":

        en_certs = [
            "toefl",
            "test of english as a foreign language",
            "ielts",
            "cambridge english",
            "b2 first",
            "first certificate",
            "linguaskill",
            "language test report form",
            "english language test",
        ]
        if any(kw in text_low for kw in en_certs):
            score += 5

        if "overall band" in text_low or "overall score" in text_low:
            score += 2

    return score


def score_degree_certificate(text_low: str, text_norm: str) -> int:
    score = 0

    degree_keywords = [
        "bachelorzeugnis",
        "zeugnis",
        "urkunde",
        "bachelor of science",
        "bachelor of arts",
        "bachelor of engineering",
        "bachelor of",
        "degree certificate",
        "degree",
        "diploma",
        "baccalaureate",
        "this is to certify that",
        "has been awarded the degree",
    ]
    if any(kw in text_low for kw in degree_keywords):
        score += 4

    if any(kw in text_low for kw in ["gesamtnote", "abschlussnote", "overall grade"]):
        score += 2

    if "transcript" not in text_low and "ects" not in text_low and "credits" not in text_low:
        score += 1

    return score


def score_vpd(text_low: str) -> int:
    score = 0

    vpd_keywords = [
        "vorprüfungsdokumentation",
        "vorpruefungsdokumentation",
        "vpd",
        "uni-assist",
        "uni assist",
    ]
    if any(kw in text_low for kw in vpd_keywords):
        score += 6

    if "bewertung" in text_low and "ausländischer hochschulabschluss" in text_low:
        score += 2

    return score


def classify_document(pdf_path: str, program: str):
    print("Hmm...")
    text = quick_ocr_preview(pdf_path, dpi=100, psm=6, max_pages=2)
    if not text.strip():
        return "other", {"transcript": 0, "language_certificate": 0,
                         "degree_certificate": 0, "vpd": 0}

    text_low = text.lower()
    text_norm = normalize_text(text)

    scores = {}
    scores["transcript"] = score_transcript(text_low, text_norm)
    scores["language_certificate"] = score_language_cert(text_low, program)
    scores["degree_certificate"] = score_degree_certificate(
        text_low, text_norm)
    scores["vpd"] = score_vpd(text_low)

    best_type = max(scores, key=scores.get)
    best_score = scores[best_type]

    if best_score < 2:
        doc_type = "other"
    else:
        doc_type = best_type

    return doc_type, scores


def classify_many(pdf_paths, program: str):

    by_type = {
        "transcript": [],
        "language_certificate": [],
        "degree_certificate": [],
        "vpd": [],
        "other": [],
    }
    best_transcript = (None, None)
    best_transcript_score = -1

    print("Aha....")
    for p in pdf_paths:
        doc_type, scores = classify_document(p, program)
        by_type.setdefault(doc_type, []).append(p)
        if doc_type == "transcript":
            sc = scores.get("transcript", 0)
            if sc > best_transcript_score:
                best_transcript_score = sc
                best_transcript = (p, scores)

    return {
        "by_type": by_type,
        "best_transcript": best_transcript,
    }
