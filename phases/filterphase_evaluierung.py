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

    paths = init_paths_from_config(config)
    try:
        ensure_ocr_available()
    except RuntimeError as e:
        logging.error(f"FATAL: {e}. Breche Evaluierung ab.")
        return

    module_map = load_module_mapping(paths["module_map_csv"])
    whitelist_set = load_whitelist(getattr(config, "WHITELIST_UNIS", None))

    categories = list(getattr(config, "REQUIREMENTS", {}).keys())

    # CSV initialisieren (nur Header)
    with open(paths["output_csv"], "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = [
            "ApplicantNumber",
            "Decision",
            "Reason",
            "BachelorCountry",
            "UniversityName",
            "UniversityWhitelisted",
            "HasVPD",
            "HasBachelorCertificate",
            "HasTranscript",
            "OtherDocuments",
            "Claimed_Grade",
            "OCR_Grade",
            "Grade_Source",
        ]
        header.extend([f"Claimed_{c}" for c in categories])
        header.extend([f"OCR_{c}" for c in categories])
        header.extend(
            [
                "MatchedModules",
                "UnrecognizedLines",
                "Extraction_Method",
            ]
        )
        header.append("Evaluation_Time_Seconds")
        writer.writerow(header)

    try:
        # --- STEP 0: Safety Check ---
        # Wait until there are at least 3 "Operator" rows on the screen.
        # This prevents the script from running before the "Add" button click has finished.
        WebDriverWait(bot.browser, 1).until(
            lambda driver: len(driver.find_elements(By.CLASS_NAME, "dropdownEqualOperator")) >= 4
        )

        # Define the operator you want: "=" or "≠"
        target_operator = "≠" 
        
        # --- STEP 1: Click the 3rd Operator Dropdown ---
        # Strategy: Find the 3rd container div, then find the dropdown inside it.
        operator_trigger = WebDriverWait(bot.browser, 1).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "(//div[contains(@class, 'dropdownEqualOperator')])[4]//div[contains(@class, 'ui-selectonemenu')]"
                )
            )
        )
        bot.browser.execute_script("arguments[0].scrollIntoView({block: 'center'});", operator_trigger)
        operator_trigger.click()
        
        # Click the operator option
        operator_option = WebDriverWait(bot.browser, 1).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    f"//li[contains(@class, 'ui-selectonemenu-item')][normalize-space()='{target_operator}']"
                )
            )
        )
        operator_option.click()
        
        # --- STEP 2: Click the 3rd Status Dropdown ---
        target_status_text = "In Vorbereitung" 
        
        # Strategy: Find the 3rd Operator container again, and look for the 
        # dropdown that is its IMMEDIATE NEIGHBOR (following-sibling).
        status_trigger = WebDriverWait(bot.browser, 1).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "(//div[contains(@class, 'dropdownEqualOperator')])[4]/following-sibling::div[contains(@class, 'ui-selectonemenu')]"
                )
            )
        )
        status_trigger.click()
        
        # Click the status option
        status_option = WebDriverWait(bot.browser, 1).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    f"//li[contains(@class, 'ui-selectonemenu-item')][normalize-space()='{target_status_text}']"
                )
            )
        )
        status_option.click()

    except Exception as e:
        logging.error(f"Error in dropdown selection: {e}")
        rows_found = len(bot.browser.find_elements(By.CLASS_NAME, "dropdownEqualOperator"))
        logging.debug(f"Found {rows_found} operator rows.")
        return
    
    try:
        search_btn = WebDriverWait(bot.browser, 1).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[.//span[normalize-space()='Suchen']]"
                    " | //span[normalize-space()='Suchen']/parent::button"
                    " | //button[contains(@id,'search')]",
                )
            )
        )
        bot.browser.execute_script(
            "arguments[0].scrollIntoView(true);", search_btn
        )
        time.sleep(0.1)
        bot.browser.execute_script("arguments[0].click();", search_btn)
        logging.debug("Warte auf Ergebnisse...")
        WebDriverWait(bot.browser, 1).until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, "span.dataScrollerResultText")
            )
        )

    except Exception as e:
        logging.error(f"{e}")
        return

    try:
        rows_initial = bot.browser.find_elements(*ROW_LOCATOR)
        candidate_rows = [r for r in rows_initial if is_candidate_row(r)]
        total = len(candidate_rows)
        logging.debug(f"TRACE: {total} Zeilen erkannt")
    except Exception as count_e:
        logging.error(f"Konnte Zeilen nicht finden {count_e}")
        return

    if total == 0:
        logging.info("Keine Bewerber gefunden.")
        return

    main_window_handle = bot.browser.current_window_handle
    logging.debug(f"Handle: {main_window_handle}")

    module_csv_path = paths["module_map_csv"]
    if "mathemodule" in module_csv_path.lower():
        program = "ai"
    else:
        program = "bwl"

    for i in tqdm.tqdm(range(total), desc="Processing", unit="application"):
        app_start = time.time()  # start timing for this applicant

        applicant_num_from_list = f"unknown_idx_{i}"
        applicant_num = applicant_num_from_list

        ocr_note = None
        saved_pdf_counts = {cat: 0.0 for cat in categories}
        matched_modules = []
        unrecognized_lines = []
        extraction_method = "N/A"
        pdfs = []
        is_non_eu = False
        has_vpd = False
        has_bachelor_certificate = False
        has_transcript = False
        other_documents = []
        uni_name_from_dom = ""
        language_status = "not evaluated"
        decision = "No"
        details_list = []
        note_source = "None"
        status = "Nicht erfuellt"

        try:
            logging.debug(f" Verarbeitung {i+1}/{total} (Index {i})")

            if bot.browser.current_window_handle != main_window_handle:
                bot.browser.switch_to.window(main_window_handle)
            time.sleep(0.5)

            rows = WebDriverWait(bot.browser, 2).until(
                EC.presence_of_all_elements_located(ROW_LOCATOR)
            )
            candidate_rows = [r for r in rows if is_candidate_row(r)]
            if i >= len(candidate_rows):
                break

            current_row = candidate_rows[i]

            try:
                td_num = current_row.find_element(
                    By.XPATH,
                    ".//td[contains(@class,'column3') or contains(@class,'column 3')][1]",
                )
                row_text = td_num.text.strip()
                mnum = BEWERBERNUMMER.search(row_text)
                if mnum:
                    applicant_num_from_list = mnum.group(1)
                    applicant_num = applicant_num_from_list
            except Exception:
                pass

            # Detail-Link/Button finden
            url_to_open = None
            element_for_js_click = None
            try:
                link_element = current_row.find_element(
                    By.XPATH,
                    ".//a[contains(@href,'applicationEditor-flow')]",
                )
                url_to_open = link_element.get_attribute("href")
                element_for_js_click = link_element
            except NoSuchElementException:
                try:
                    button_element = current_row.find_element(
                        By.XPATH,
                        ".//button[contains(@id,'tableRowAction') or contains(@name,'tableRowAction')]",
                    )
                    element_for_js_click = button_element
                except NoSuchElementException:
                    logging.error(
                        f"kein Button für Bewerber {applicant_num_from_list} gefunden"
                    )
                    continue

            initial_handles = set(bot.browser.window_handles)

            if url_to_open:
                bot.browser.execute_script(
                    f"window.open('{url_to_open}', '_blank');"
                )
            elif element_for_js_click:
                bot.browser.execute_script(
                    "arguments[0].click();", element_for_js_click
                )
            else:
                logging.error("Kein Element zum Klicken")
                continue

            time.sleep(1)
            current_handles = bot.browser.window_handles
            new_handles = set(current_handles) - initial_handles

            if not new_handles and "applicationEditor-flow" not in bot.browser.current_url:
                logging.error("Neuer Tab nicht geoeffnet")
                continue
            elif not new_handles and "applicationEditor-flow" in bot.browser.current_url:
                new_tab_handle = main_window_handle
            else:
                new_tab_handle = list(new_handles)[0]
                bot.browser.switch_to.window(new_tab_handle)

            # popup handling
            if i == 0:
                time.sleep(1)

            WebDriverWait(bot.browser, 2).until(
                lambda d: d.execute_script("return document.readyState")
                == "complete"
            )
            time.sleep(1)

            applicant_num = get_applicant_number_from_detail_page(bot.browser)
            logging.debug(
                f"Aktuelle Bewerbernummer im Detail-Tab: {applicant_num}"
            )

            # Antrags-Button (falls mehrere Anträge)
            try:
                application_buttons = bot.browser.find_elements(
                    By.XPATH,
                    "//button[contains(@id, 'showRequestSubjectBtn')]",
                )
                if application_buttons:
                    btn_text = application_buttons[0].text
                    logging.info(
                        f"{len(application_buttons)} Wähle ersten: '{btn_text}'"
                    )
                    bot.browser.execute_script(
                        "arguments[0].click();", application_buttons[0]
                    )
                    WebDriverWait(bot.browser, 2).until(
                        lambda d: d.execute_script(
                            "return document.readyState"
                        )
                        == "complete"
                    )
                    time.sleep(0.5)
            except Exception as e:
                logging.error(f"{e}")

            # DOM-Infos (Claimed, Uni, Country)
            claimed = extract_claimed_from_dom(bot.browser, config)
            uni_name_from_dom = get_university_from_dom(bot.browser)
            bachelor_country = claimed.get("bachelor_country")
            bachelor_country_raw = claimed.get("bachelor_country_raw")

            # (A) vs (D)
            try:
                bot.browser.find_element(
                    By.XPATH,
                    "//h2[contains(., 'Masterzugangsberechtigung (A)')]",
                )
                is_non_eu = True
                logging.info("Applicant classified as Non-EU (A).")
            except NoSuchElementException:
                is_non_eu = False
                logging.info("Applicant classified as German/EU (D).")

            # pdf download
            pdfs = download_pdfs_for_applicant(
                browser=bot.browser,
                download_dir=paths["download_dir"],
                extract_dir=paths["extract_dir"],
                applicant_num=applicant_num,
            )

            has_vpd = False
            has_bachelor_certificate = False
            has_transcript = False
            other_documents = []
            language_status = "not evaluated"

            # OCR grade
            vpd_pdfs = []
            grade_pdfs = []

            if pdfs:
                vpd_pdfs = [
                    p for p in pdfs if "vpd" in os.path.basename(p).lower()
                ]
                grade_keywords = [
                    "zeugnis",
                    "certificate",
                    "urkunde",
                    "diploma",
                ]
                grade_pdfs = [
                    p
                    for p in pdfs
                    if any(
                        kw in os.path.basename(p).lower()
                        for kw in grade_keywords
                    )
                ]

            if vpd_pdfs:
                has_vpd = True
                logging.info("VPD  found")
                vpd_pdf = vpd_pdfs[0]
                text_vpd = ocr_text_from_pdf(vpd_pdf)
                if not text_vpd:
                    ocr_note = None
                else:
                    ocr_note = extract_ocr_note(text_vpd)
            elif is_non_eu:
                ocr_note = None
            else:
                logging.info("No VPD found")
                combined_text = ""
                for gpdf in grade_pdfs:
                    combined_text += "\n" + (ocr_text_from_pdf(gpdf) or "")
                if not combined_text.strip():
                    ocr_note = None
                else:
                    ocr_note = extract_ocr_note(combined_text)
                if ocr_note is None and pdfs:
                    main_pdf_path_for_note = max(pdfs, key=os.path.getsize)
                    logging.info(" Fallback OCR on largest PDF")
                    fallback_text = ocr_text_from_pdf(main_pdf_path_for_note)
                    if not fallback_text:
                        ocr_note = None
                    else:
                        ocr_note = extract_ocr_note(fallback_text)

            claimed_note = claimed.get("note")
            if ocr_note is not None:
                note_used = ocr_note
                note_source = "OCR"
                if (
                    claimed_note is not None
                    and abs(ocr_note - claimed_note) >= 0.1
                ):
                    details_list.append(
                        f"Grade mismatch between DOM and OCR (claimed: {claimed_note}, OCR: {ocr_note})"
                    )
            else:
                note_used = claimed_note
                note_source = "Claimed" if note_used is not None else "None"

            # prototype: grade verification (Bavarian)
            if not has_vpd:
                foreign_grade = ocr_note
                claimed_german = claimed_note
                if (
                    bachelor_country
                    and foreign_grade is not None
                    and claimed_german is not None
                ):
                    is_consistent, converted, bav_reason = verify_grade(
                        bachelor_country, foreign_grade, claimed_german
                    )
                    if (
                        is_consistent is False
                        and converted is not None
                        and bav_reason == "BavarianMismatch"
                    ):
                        details_list.append("BavarianMismatch")

            note_ok = True
            req_max = getattr(config, "REQ_NOTE_MAX", 2.4)
            if note_used is None:
                details_list.append(
                    f"No usable grade found (source: {note_source})."
                )
                note_ok = False
            else:
                if note_used > req_max:
                    details_list.append(
                        f"Grade too low ({note_used} > {req_max})."
                    )
                    note_ok = False

            # classifier prototype
            non_vpd_pdfs = (
                [p for p in pdfs if "vpd" not in os.path.basename(p).lower()]
                if pdfs
                else []
            )
            transcript_candidates = []
            degree_pdfs = []
            lang_pdfs = []
            other_pdfs = []
            best_transcript_path = None

            if non_vpd_pdfs:
                class_result = classify_many(non_vpd_pdfs, program)
                by_type = class_result["by_type"]
                best_transcript_path, _scores = class_result["best_transcript"]

                transcript_candidates = by_type.get("transcript", [])
                degree_pdfs = by_type.get("degree_certificate", [])
                lang_pdfs = by_type.get("language_certificate", [])
                for dtype, paths_list in by_type.items():
                    if dtype not in (
                        "transcript",
                        "degree_certificate",
                        "language_certificate",
                        "vpd",
                    ):
                        other_pdfs.extend(paths_list)

            has_bachelor_certificate = bool(degree_pdfs)
            has_transcript = bool(transcript_candidates or best_transcript_path)
            other_documents = [os.path.basename(p) for p in other_pdfs]

            # language certificates
            if program == "bwl":
                language_status = evaluate_language_status_bwl(
                    lang_pdfs, bachelor_country_raw or bachelor_country or ""
                )
            else:
                language_status = evaluate_language_status_ai(lang_pdfs)
            details_list.append(
                f"Language certificate status: {language_status}"
            )

            # Whitelist
            is_whitelisted, uni_match = check_university_whitelist(
                uni_name_from_dom, whitelist_set
            )

            if is_whitelisted:
                logging.info(
                    f"Applicant {applicant_num} is on the whitelist (match: '{uni_match}')."
                )
                extraction_method = "Whitelist"
                status_ects, details_ects = evaluate_requirements_ects(
                    claimed, [], [], config
                )
                details_list.append(f"University whitelist: {uni_match}")
                details_list.append(f"ECTS (claimed) status: {status_ects}")

                if status_ects == "Fulfilled" and note_ok:
                    status = "Fulfilled"
                else:
                    status = "Not fulfilled"
            else:
                logging.info(
                    f"Applicant {applicant_num} is not on whitelist"
                )

                if not pdfs:
                    logging.error("No PDFs found ")
                    details_list.append("No PDFs for ECTS evaluation.")
                    status = "Not fulfilled"
                elif not non_vpd_pdfs:
                    logging.error(
                        "Only VPDs found, no transcript for ECTS evaluation."
                    )
                    details_list.append(
                        "Only VPD found, no transcript "
                    )
                    status = "Not fulfilled"
                else:
                    if best_transcript_path is None:
                        main_pdf_path = max(
                            non_vpd_pdfs, key=os.path.getsize
                        )
                        details_list.append(
                            "No clear transcript detected largest PDF = ECTS."
                        )
                    else:
                        main_pdf_path = best_transcript_path
                        details_list.append(
                            f"Transcript chosen by OCR classifier: {os.path.basename(main_pdf_path)}"
                        )

                    logging.debug(
                        f"Start hybrid ECTS extraction (OCR lab) for main PDF: {os.path.basename(main_pdf_path)}"
                    )
                    sums, matched, unrec, method = extract_ects_hybrid(
                        main_pdf_path,
                        module_map,
                        categories,
                    )

                    saved_pdf_counts = sums
                    matched_modules = matched
                    unrecognized_lines = unrec
                    extraction_method = method

                    status_ects, details_ects = evaluate_requirements_ects(
                        saved_pdf_counts,
                        matched_modules,
                        unrecognized_lines,
                        config,
                    )
                    details_list.append(f"ECTS (OCR) status: {status_ects}")

                    if status_ects == "Fulfilled" and note_ok:
                        status = "Fulfilled"
                    else:
                        status = "Not fulfilled"

            if is_non_eu and not has_vpd:
                details_list.append(
                    "Documents insufficient: VPD missing for Non-EU applicant."
                )

            decision = "Yes" if status == "Fulfilled" else "No"

        except Exception as e:
            logging.error(f"{applicant_num}: {e}")
            details_list.append(f"Evaluation error: {e}")
            decision = "No"

        # build CSV row
        details_str = "; ".join(details_list)
        csv_row = [
            applicant_num,
            decision,
            details_str,
            bachelor_country if "bachelor_country" in locals() else "",
            uni_name_from_dom,
            "Yes" if "is_whitelisted" in locals() and is_whitelisted else "No",
            "Yes" if has_vpd else "No",
            "Yes" if has_bachelor_certificate else "No",
            "Yes" if has_transcript else "No",
            ", ".join(other_documents),
            claimed.get("note") if "claimed" in locals() else None,
            ocr_note,
            note_source,
        ]

        for cat in categories:
            csv_row.append(
                claimed.get(cat, 0.0) if "claimed" in locals() else 0.0
            )

        for cat in categories:
            csv_row.append(saved_pdf_counts.get(cat, 0.0))

        csv_row.extend(
            [
                " | ".join(matched_modules),
                " | ".join(unrecognized_lines),
                extraction_method,
            ]
        )

        # per-applicant duration
        app_duration = time.time() - app_start
        csv_row.append(round(app_duration, 3))

        with open(paths["output_csv"], "a", newline="", encoding="utf-8") as of:
            writer = csv.writer(of)
            writer.writerow(csv_row)

        logging.debug(
            f"Result for {applicant_num} written. Decision: {decision}. Details: {details_str}"
        )

        # zurück zum main tab
        try:
            if bot.browser.current_window_handle != main_window_handle:
                logging.debug(
                    f"Schließe Tab {bot.browser.current_window_handle}..."
                )
                bot.browser.close()
                bot.browser.switch_to.window(main_window_handle)
                logging.debug(f"back zum Haupt-Tab {main_window_handle}.")
        except Exception as fe:
            logging.error(f"Fehler beim Tab-Schließen: {fe}")

    total_time = time.time() - eval_start
    logging.debug(f"Total evaluation time: {total_time:.2f} seconds")
    logging.debug(f"abgeschlossen. CSV: {paths['output_csv']}")
