import os
import re
import time
import csv
import tqdm
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

from utils.document_classifier import classify_many
from utils.language_certificates import (
    evaluate_language_status_bwl,
    evaluate_language_status_ai,
)
from utils.ocr_ects import (
    ensure_ocr_available,
    extract_ects_hybrid,
    extract_ocr_note,
    ocr_text_from_pdf,
)
from utils.claimed_dom_extract import (
    extract_claimed_from_dom,
    get_university_from_dom,
)
from utils.hisinone_downloader import download_pdfs_for_applicant
from utils.grading_systems import verify_grade


ROW_LOCATOR = (
    By.XPATH,
    "//table//tr[.//td and not(contains(@style,'display:none'))]",
)

BEWERBERNUMMER = re.compile(r"\b(\d{5,})\b")

def init_paths_from_config(config):
    base_dir = os.path.dirname(__file__)
    ressources_dir = os.path.abspath(
        os.path.join(base_dir, "..", "ressources")
    )

    download_dir = getattr(
        config,
        "DOWNLOAD_DIR",
        os.path.join(ressources_dir, "downloads"),
    )
    extract_dir = getattr(
        config,
        "EXTRACT_DIR",
        os.path.join(download_dir, "extracted"),
    )
    module_map_csv = getattr(
        config,
        "MODULE_MAP_CSV",
        os.path.join(ressources_dir, "modul_mengen_stat_vwl_bwl.csv"),
    )
    output_csv = getattr(
        config,
        "OUTPUT_CSV",
        os.path.join(ressources_dir, "bewerber_evaluierung.csv"),
    )

    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    return {
        "ressources_dir": ressources_dir,
        "download_dir": download_dir,
        "extract_dir": extract_dir,
        "module_map_csv": module_map_csv,
        "output_csv": output_csv,
    }


def load_whitelist(csv_path):
    whitelist = set()
    if not csv_path or not os.path.exists(csv_path):
        logging.warn("Keine Whitelist-Datei angegeben.")
        return whitelist
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row and row[0].strip():
                whitelist.add(row[0].strip().lower())
    logging.info(f"Whitelist geladen: {len(whitelist)} Einträge.")
    return whitelist


def load_module_mapping(csv_path):
    mapping = {}
    if not os.path.exists(csv_path):
        logging.warn(f"Mapping  fehlt: {csv_path}")
        return mapping
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            key = r.get("module") or r.get("modul")
            cat = r.get("category") or r.get("Kategorie")
            if key and cat:
                mapping[key.strip().lower()] = cat.strip()
    logging.info(f"Modul-Mapping geladen: {len(mapping)} eintraege.")
    return dict(
        sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True)
    )


def is_candidate_row(row):
    try:
        cells = row.find_elements(By.TAG_NAME, "td")
        if not cells or len(cells) < 3:
            return False
        text = " ".join(c.text.strip().lower() for c in cells)
        return ("bewerbung" in text) or BEWERBERNUMMER.search(text)
    except Exception:
        return False


def get_applicant_number_from_detail_page(browser):
    try:
        el = WebDriverWait(browser, 1).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//span[contains(@id, 'applicantDataSummary_number')]"
                    " | //span[contains(text(), 'Bewerbernummer')]/following-sibling::span"
                    " | //span[contains(text(), 'Bewerbungsnummer')]/following-sibling::span",
                )
            )
        )
        txt = el.text.strip()
        m = BEWERBERNUMMER.search(txt)
        if m:
            return m.group(1)
        return f"unknown_{int(time.time())}"
    except Exception:
        return f"unknown_{int(time.time())}"


def check_university_whitelist(uni_text: str, whitelist_set):
    if not whitelist_set or not uni_text:
        return False, None

    low = uni_text.lower()
    for uni_name in whitelist_set:
        if uni_name in low:
            return True, uni_name
    return False, None


def evaluate_requirements_ects(ects_data, matched_modules, unrecognized, config):
    """
    Only checks ECTS requirements (grade is checked separately).

    ects_data: dict[category -> float]
    """
    reasons = []
    ok = True
    requirements_ects = getattr(config, "REQUIREMENTS", {})

    if not requirements_ects:
        reasons.append("No ECTS requirements defined in config.")
        ok = False
    else:
        for category, req_value in requirements_ects.items():
            ocr_value = float(ects_data.get(category, 0.0))
            if ocr_value < req_value:
                reasons.append(
                    f"{category}: not enough ECTS ({ocr_value} < {req_value})"
                )
                ok = False

    if unrecognized:
        reasons.append(f"{len(unrecognized)} unrecognized module line(s)")

    status = "Fulfilled" if ok else "Not fulfilled"
    details = "All ECTS criteria fulfilled." if not reasons else "; ".join(
        reasons
    )
    return status, details

def run_filterphase_evaluierung(bot, flow_url, config):
    logging.info("Starte Evaluierung...")
    eval_start = time.time()

    # 1. Setup Resources
    paths = init_paths_from_config(config)
    try:
        ensure_ocr_available()
    except RuntimeError as e:
        logging.error(f"FATAL: {e}. Breche Evaluierung ab.")
        return

    module_map = load_module_mapping(paths["module_map_csv"])
    whitelist_set = load_whitelist(getattr(config, "WHITELIST_UNIS", None))
    categories = list(getattr(config, "REQUIREMENTS", {}).keys())

    # 2. Initialize CSV
    _init_csv_file(paths["output_csv"], categories)

    # 3. Apply UI Filters & Search
    if not _apply_search_filters(bot):
        return
    
    if not _trigger_search_and_wait(bot):
        return

    # 4. OPTIMIZED: Identify Candidate Indices ONCE
    try:
        rows_initial = bot.browser.find_elements(*ROW_LOCATOR)
        # Store only the integer index of rows that match criteria
        candidate_indices = [
            idx for idx, r in enumerate(rows_initial) 
            if idx > 0 and is_candidate_row(r)
        ]
        total = len(candidate_indices)
        logging.debug(f"TRACE: {total} Zeilen erkannt (Indices: {candidate_indices})")
    except Exception as count_e:
        logging.error(f"Konnte Zeilen nicht finden {count_e}")
        return

    if total == 0:
        logging.info("Keine Bewerber gefunden.")
        return

    main_window_handle = bot.browser.current_window_handle
    
    # Determine Program Type
    program = "ai" if "mathemodule" in paths["module_map_csv"].lower() else "bwl"

    # 5. Main Processing Loop (Iterate over Indices)
    for loop_index, target_row_index in enumerate(tqdm.tqdm(candidate_indices, desc="Processing", unit="app")):
        app_start = time.time()
        
        # Pass both the loop_index (for counting 1/100) and target_row_index (for locating the row)
        result_data = _process_single_applicant(
            bot, loop_index, target_row_index, main_window_handle, paths, 
            module_map, whitelist_set, categories, program, config
        )

        if not result_data:
            continue 

        # Calculate Duration & Write CSV
        result_data["duration"] = round(time.time() - app_start, 3)
        _write_result_to_csv(paths["output_csv"], result_data, categories)

    total_time = time.time() - eval_start
    logging.debug(f"Total evaluation time: {total_time:.2f} seconds")
    logging.debug(f"abgeschlossen. CSV: {paths['output_csv']}")


# ==============================================================================
#                               HELPER FUNCTIONS
# ==============================================================================

def _process_single_applicant(bot, loop_index, target_row_index, main_window_handle, paths, module_map, whitelist_set, categories, program, config):
    """
    Processes the applicant at the specific physical DOM index `target_row_index`.
    """
    
    # Default Result Structure
    res = {
        "applicant_num": f"unknown_idx_{loop_index}",
        "decision": "No",
        "details_list": [],
        "claimed": {},
        "saved_pdf_counts": {cat: 0.0 for cat in categories},
        "matched_modules": [],
        "unrecognized_lines": [],
        "extraction_method": "N/A",
        "has_vpd": False,
        "has_bachelor": False,
        "has_transcript": False,
        "other_docs": [],
        "ocr_note": None,
        "note_source": "None",
        "bachelor_country": "",
        "uni_name": "",
        "is_whitelisted": False,
        "note_ok": False,
        "status_final": "Not fulfilled"
    }

    try:
        # 1. Navigation: Open Tab using the Index
        if not _navigate_to_applicant_detail_by_index(bot, target_row_index, main_window_handle, res):
            return None 

        # 2. Extract Metadata
        res["applicant_num"] = get_applicant_number_from_detail_page(bot.browser)
        _handle_application_buttons(bot)
        
        res["claimed"] = extract_claimed_from_dom(bot.browser, config)
        res["uni_name"] = get_university_from_dom(bot.browser)
        res["bachelor_country"] = res["claimed"].get("bachelor_country", "")
        
        is_non_eu = _check_non_eu_status(bot)
        
        # 3. Document Download
        pdfs = download_pdfs_for_applicant(
            browser=bot.browser,
            download_dir=paths["download_dir"],
            extract_dir=paths["extract_dir"],
            applicant_num=res["applicant_num"],
        )

        # 4. Analyze Grades
        _analyze_grade_logic(pdfs, is_non_eu, res, config)

        # 5. Analyze ECTS / Documents
        _analyze_documents_and_ects(pdfs, program, is_non_eu, module_map, whitelist_set, categories, res, config)
        
        # 6. Final Decision
        if res.get("status_final") == "Fulfilled":
            res["decision"] = "Yes"
        
        # Cleanup
        _close_tab_and_return(bot, main_window_handle)
        return res

    except Exception as e:
        logging.error(f"{res['applicant_num']}: {e}")
        res["details_list"].append(f"Evaluation error: {e}")
        _close_tab_and_return(bot, main_window_handle)
        return res


def _navigate_to_applicant_detail_by_index(bot, target_index, main_window_handle, res):
    """
    Refetches the table (to avoid StaleElement) and clicks the row at `target_index`.
    Does NOT run is_candidate_row() again.
    """
    try:
        if bot.browser.current_window_handle != main_window_handle:
            bot.browser.switch_to.window(main_window_handle)
        time.sleep(0.1)

        # Re-fetch ALL rows
        rows = WebDriverWait(bot.browser, 2).until(EC.presence_of_all_elements_located(ROW_LOCATOR))
        
        # Safety check: Table size shouldn't have shrunk
        if target_index >= len(rows):
            logging.error(f"Table index {target_index} out of bounds (found {len(rows)} rows).")
            return False
            
        # DIRECT ACCESS - No Filtering
        current_row = rows[target_index]
        
        # Extract ID from list view (optional, just for fallback)
        try:
            td_num = current_row.find_element(By.XPATH, ".//td[contains(@class,'column3') or contains(@class,'column 3')][1]")
            mnum = BEWERBERNUMMER.search(td_num.text.strip())
            if mnum: res["applicant_num"] = mnum.group(1)
        except: pass

        # Find Link/Button
        url_to_open = None
        click_element = None
        try:
            link = current_row.find_element(By.XPATH, ".//a[contains(@href,'applicationEditor-flow')]")
            url_to_open = link.get_attribute("href")
        except NoSuchElementException:
            try:
                click_element = current_row.find_element(By.XPATH, ".//button[contains(@id,'tableRowAction')]")
            except NoSuchElementException:
                logging.error(f"Kein Button für {res['applicant_num']}")
                return False

        # Open
        initial_handles = set(bot.browser.window_handles)
        if url_to_open:
            bot.browser.execute_script(f"window.open('{url_to_open}', '_blank');")
        elif click_element:
            bot.browser.execute_script("arguments[0].click();", click_element)

        time.sleep(0.1)
        new_handles = set(bot.browser.window_handles) - initial_handles
        
        if not new_handles and "applicationEditor-flow" in bot.browser.current_url:
            # Opened in same tab
            new_tab = main_window_handle
        elif new_handles:
            # Opened in new tab
            new_tab = list(new_handles)[0]
            bot.browser.switch_to.window(new_tab)
        else:
            return False

        WebDriverWait(bot.browser, 2).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(0.1)
        return True

    except Exception as e:
        logging.error(f"Navigation error: {e}")
        return False


def _analyze_grade_logic(pdfs, is_non_eu, res, config):
    ocr_note = None
    has_vpd = False
    
    vpd_pdfs = [pdf_path for pdf_path in pdfs if "vpd" in os.path.basename(pdf_path).lower()]
    
    grade_keywords = ["zeugnis", "certificate", "urkunde", "diploma"]
    grade_pdfs = [
        pdf_path for pdf_path in pdfs 
        if any(kw in os.path.basename(pdf_path).lower() for kw in grade_keywords)
    ]

    if vpd_pdfs:
        has_vpd = True
        logging.info("VPD found")
        text_vpd = ocr_text_from_pdf(vpd_pdfs[0])
        ocr_note = extract_ocr_note(text_vpd) if text_vpd else None
    elif not is_non_eu:
        combined_text = "\n".join([(ocr_text_from_pdf(pdf_path) or "") for pdf_path in grade_pdfs])
        ocr_note = extract_ocr_note(combined_text) if combined_text.strip() else None
        
        if ocr_note is None and pdfs:
            # Fallback to largest PDF
            fallback_pdf = max(pdfs, key=os.path.getsize)
            fallback_text = ocr_text_from_pdf(fallback_pdf)
            ocr_note = extract_ocr_note(fallback_text) if fallback_text else None

    res["has_vpd"] = has_vpd
    res["ocr_note"] = ocr_note
    
    claimed_note = res["claimed"].get("note")
    note_used = None
    
    if ocr_note is not None:
        note_used = ocr_note
        res["note_source"] = "OCR"
        if claimed_note and abs(ocr_note - claimed_note) >= 0.1:
            res["details_list"].append(f"Grade mismatch (claimed: {claimed_note}, OCR: {ocr_note})")
    else:
        note_used = claimed_note
        res["note_source"] = "Claimed" if note_used else "None"

    if not has_vpd and res["bachelor_country"] and ocr_note and claimed_note:
        is_consistent, converted, bav_reason = verify_grade(res["bachelor_country"], ocr_note, claimed_note)
        if is_consistent is False and bav_reason == "BavarianMismatch":
            res["details_list"].append("BavarianMismatch")

    res["note_ok"] = True
    req_max = getattr(config, "REQ_NOTE_MAX", 2.4)
    
    if note_used is None:
        res["details_list"].append(f"No usable grade found (source: {res['note_source']}).")
        res["note_ok"] = False
    elif note_used > req_max:
        res["details_list"].append(f"Grade too low ({note_used} > {req_max}).")
        res["note_ok"] = False


def _analyze_documents_and_ects(pdfs, program, is_non_eu, module_map, whitelist_set, categories, res, config):
    non_vpd_pdfs = [pdf_path for pdf_path in pdfs if "vpd" not in os.path.basename(pdf_path).lower()]
    best_transcript_path = None
    lang_pdfs = []
    
    if non_vpd_pdfs:
        class_result = classify_many(non_vpd_pdfs, program)
        best_transcript_path, _ = class_result["best_transcript"]
        
        res["has_bachelor"] = bool(class_result["by_type"].get("degree_certificate"))
        res["has_transcript"] = bool(class_result["by_type"].get("transcript") or best_transcript_path)
        lang_pdfs = class_result["by_type"].get("language_certificate", [])
        
        for dtype, paths_list in class_result["by_type"].items():
            if dtype not in ("transcript", "degree_certificate", "language_certificate", "vpd"):
                res["other_docs"].extend([os.path.basename(pdf_path) for pdf_path in paths_list])

    if program == "bwl":
        lang_status = evaluate_language_status_bwl(lang_pdfs, res.get("bachelor_country_raw", ""))
    else:
        lang_status = evaluate_language_status_ai(lang_pdfs)
    res["details_list"].append(f"Language status: {lang_status}")

    is_whitelisted, uni_match = check_university_whitelist(res["uni_name"], whitelist_set)
    res["is_whitelisted"] = is_whitelisted
    status_ects = "Not fulfilled"

    if is_whitelisted:
        logging.info(f"Whitelisted match: {uni_match}")
        res["extraction_method"] = "Whitelist"
        status_ects, _ = evaluate_requirements_ects(res["claimed"], [], [], config)
        res["details_list"].append(f"University whitelist: {uni_match}")
        res["details_list"].append(f"ECTS (claimed) status: {status_ects}")
    else:
        if not pdfs:
            res["details_list"].append("No PDFs for ECTS evaluation.")
        elif not non_vpd_pdfs:
            res["details_list"].append("Only VPD found, no transcript.")
        else:
            main_pdf = best_transcript_path if best_transcript_path else max(non_vpd_pdfs, key=os.path.getsize)
            if not best_transcript_path:
                res["details_list"].append("No clear transcript detected, using largest PDF.")

            sums, matched, unrec, method = extract_ects_hybrid(main_pdf, module_map, categories)
            
            res["saved_pdf_counts"] = sums
            res["matched_modules"] = matched
            res["unrecognized_lines"] = unrec
            res["extraction_method"] = method
            
            status_ects, _ = evaluate_requirements_ects(sums, matched, unrec, config)
            res["details_list"].append(f"ECTS (OCR) status: {status_ects}")

    if status_ects == "Fulfilled" and res["note_ok"]:
        res["status_final"] = "Fulfilled"
    else:
        res["status_final"] = "Not fulfilled"

    if is_non_eu and not res["has_vpd"]:
        res["details_list"].append("Documents insufficient: VPD missing for Non-EU applicant.")

        
def _apply_search_filters(bot):
    try:
        WebDriverWait(bot.browser, 1).until(lambda d: len(d.find_elements(By.CLASS_NAME, "dropdownEqualOperator")) >= 4)
        
        # 3rd Operator -> ≠
        op_trig = WebDriverWait(bot.browser, 1).until(EC.element_to_be_clickable(
            (By.XPATH, "(//div[contains(@class, 'dropdownEqualOperator')])[4]//div[contains(@class, 'ui-selectonemenu')]")))
        bot.browser.execute_script("arguments[0].scrollIntoView({block: 'center'});", op_trig)
        op_trig.click()
        WebDriverWait(bot.browser, 1).until(EC.element_to_be_clickable(
            (By.XPATH, "//li[contains(@class, 'ui-selectonemenu-item')][normalize-space()='≠']"))).click()
        
        # 3rd Status -> In Vorbereitung
        stat_trig = WebDriverWait(bot.browser, 1).until(EC.element_to_be_clickable(
            (By.XPATH, "(//div[contains(@class, 'dropdownEqualOperator')])[4]/following-sibling::div[contains(@class, 'ui-selectonemenu')]")))
        stat_trig.click()
        WebDriverWait(bot.browser, 1).until(EC.element_to_be_clickable(
            (By.XPATH, "//li[contains(@class, 'ui-selectonemenu-item')][normalize-space()='In Vorbereitung']"))).click()
        return True
    except Exception as e:
        logging.error(f"Error in dropdown selection: {e}")
        return False

def _trigger_search_and_wait(bot):
    try:
        search_btn = WebDriverWait(bot.browser, 1).until(EC.element_to_be_clickable(
            (By.XPATH, "//button[.//span[normalize-space()='Suchen']] | //span[normalize-space()='Suchen']/parent::button | //button[contains(@id,'search')]")))
        bot.browser.execute_script("arguments[0].scrollIntoView(true);", search_btn)
        time.sleep(0.1)
        bot.browser.execute_script("arguments[0].click();", search_btn)
        
        logging.debug("Warte auf Ergebnisse...")
        WebDriverWait(bot.browser, 1).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "span.dataScrollerResultText")))
        return True
    except Exception as e:
        logging.error(f"Search failed: {e}")
        return False

def _init_csv_file(path, categories):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["ApplicantNumber", "Decision", "Reason", "BachelorCountry", "UniversityName",
            "UniversityWhitelisted", "HasVPD", "HasBachelorCertificate", "HasTranscript",
            "OtherDocuments", "Claimed_Grade", "OCR_Grade", "Grade_Source"]
        header.extend([f"Claimed_{c}" for c in categories])
        header.extend([f"OCR_{c}" for c in categories])
        header.extend(["MatchedModules", "UnrecognizedLines", "Extraction_Method", "Evaluation_Time_Seconds"])
        writer.writerow(header)

def _write_result_to_csv(path, res, categories):
    details_str = "; ".join(res["details_list"])
    row = [res["applicant_num"], res["decision"], details_str, res["bachelor_country"], res["uni_name"],
        "Yes" if res["is_whitelisted"] else "No", "Yes" if res["has_vpd"] else "No",
        "Yes" if res["has_bachelor"] else "No", "Yes" if res["has_transcript"] else "No",
        ", ".join(res["other_docs"]), res["claimed"].get("note"), res["ocr_note"], res["note_source"]]
    
    for c in categories: row.append(res["claimed"].get(c, 0.0))
    for c in categories: row.append(res["saved_pdf_counts"].get(c, 0.0))
    
    row.extend([" | ".join(res["matched_modules"]), " | ".join(res["unrecognized_lines"]), res["extraction_method"], res["duration"]])
    with open(path, "a", newline="", encoding="utf-8") as of:
        csv.writer(of).writerow(row)

def _check_non_eu_status(bot):
    try:
        bot.browser.find_element(By.XPATH, "//h2[contains(., 'Masterzugangsberechtigung (A)')]")
        logging.info("Non-EU (A).")
        return True
    except NoSuchElementException:
        logging.info("EU (D).")
        return False

def _close_tab_and_return(bot, main_handle):
    try:
        if bot.browser.current_window_handle != main_handle:
            bot.browser.close()
            bot.browser.switch_to.window(main_handle)
    except Exception as e:
        logging.error(f"Error closing tab: {e}")

def _handle_application_buttons(bot):
    try:
        btns = bot.browser.find_elements(By.XPATH, "//button[contains(@id, 'showRequestSubjectBtn')]")
        if btns:
            bot.browser.execute_script("arguments[0].click();", btns[0])
            WebDriverWait(bot.browser, 2).until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(0.1)
    except Exception: pass
