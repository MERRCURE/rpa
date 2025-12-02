import os
import glob
import time
import shutil
import zipfile
import logging
from typing import List

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def wait_for_any_file(download_dir, pattern="*.zip", timeout=40, prev=None):
    prev_set = set(prev or glob.glob(os.path.join(download_dir, pattern)))
    deadline = time.time() + timeout
    
    while time.time() < deadline:
        time.sleep(0.2) 
        current = set(glob.glob(os.path.join(download_dir, pattern)))
        new = current - prev_set
        if new:
            # Return the most recently modified file among the new ones
            return sorted(list(new), key=os.path.getmtime)[-1]
            
    return None

def extract_pdfs_from_zip(zip_path, target_dir) -> List[str]:
    """
    OPTIMIZATION: Only extracts PDF files, skipping everything else.
    Returns the list of extracted PDF paths directly.
    """
    shutil.rmtree(target_dir, ignore_errors=True)
    os.makedirs(target_dir, exist_ok=True)
    
    extracted_pdfs = []
    
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            # Filter in memory first
            file_list = z.infolist()
            for member in file_list:
                # Check extension and ignore 'deckblatt' case-insensitively
                fn_lower = member.filename.lower()
                if fn_lower.endswith(".pdf") and "deckblatt" not in fn_lower:
                    # Prevent zip slip (security) and extract
                    z.extract(member, target_dir)
                    extracted_pdfs.append(os.path.join(target_dir, member.filename))
    except zipfile.BadZipFile:
        logging.error(f"Corrupt zip file: {zip_path}")
        return []

    return extracted_pdfs

def download_pdfs_for_applicant(browser, download_dir, extract_dir, applicant_num):
    logging.debug(f"working on: Download-Button {applicant_num}")

    # OPTIMIZATION: Combine XPaths into one OR-query.
    # This checks all 4 conditions simultaneously.
    combined_xpath = (
        "//button[contains(@aria-label,'Nachweise herunterladen')] | "
        "//button[contains(@title,'Nachweise herunterladen')] | "
        "//button[.//img[contains(@src,'download.svg')]] | "
        "//img[@alt='Nachweise herunterladen']/ancestor::button"
    )

    dl_element = None
    try:
        dl_element = WebDriverWait(browser, 3).until(
            EC.element_to_be_clickable((By.XPATH, combined_xpath))
        )
        if dl_element:
            logging.debug(f"Button found immediately via combined XPath")
    except Exception:
        logging.info(f"Kein Download-Element bei {applicant_num} ")
        return []

    # Cleanup existing downloads
    with os.scandir(download_dir) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.lower().endswith((".zip", ".pdf")):
                try:
                    os.remove(entry.path)
                except OSError as e:
                    logging.warning(f"Cleanup failed for {entry.name}: {e}")

    logging.debug("click Download-Button")
    
    # Try native click first, then JS. Native is usually safer for triggering downloads.
    try:
        dl_element.click()
    except Exception:
        try:
            browser.execute_script("arguments[0].click();", dl_element)
        except Exception as e:
            logging.error(f"Click failed: {e}")
            return []

    # Snapshot state for wait
    # Note: We rely on the fact we just cleaned the folder, so any zip is new.
    # But passing prev_zips is safer for concurrency.
    prev_zips = glob.glob(os.path.join(download_dir, "*.zip"))

    zip_path = wait_for_any_file(
        download_dir,
        pattern="*.zip",
        timeout=40,
        prev=prev_zips,
    )

    if not zip_path:
        logging.error(f"no zip for {applicant_num} ")
        return []

    extract_target = os.path.join(extract_dir, f"{applicant_num}_{int(time.time())}")
    logging.debug(f"unpacking zip {extract_target}")
    
    pdfs = extract_pdfs_from_zip(zip_path, extract_target)
    
    logging.debug(f"{len(pdfs)} PDFs found: {[os.path.basename(p) for p in pdfs]}")
    return pdfs
