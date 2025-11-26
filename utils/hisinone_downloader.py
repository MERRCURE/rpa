import os
import glob
import time
import shutil
import zipfile

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def wait_for_any_file(download_dir, pattern="*.zip", timeout=40, prev=None):
    prev_set = set(prev or glob.glob(os.path.join(download_dir, pattern)))
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = set(glob.glob(os.path.join(download_dir, pattern)))
        new = current - prev_set
        if new:
            return sorted(list(new), key=lambda p: os.path.getmtime(p))[-1]
        time.sleep(0.5)
    return None


def extract_zip_to_dir(zip_path, target_dir):
    shutil.rmtree(target_dir, ignore_errors=True)
    os.makedirs(target_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(target_dir)
    return target_dir


def find_pdfs_in_dir(d):
    pdfs = glob.glob(os.path.join(d, "**", "*.pdf"), recursive=True)
    return [
        p for p in pdfs
        if "__deckblatt" not in os.path.basename(p).lower()
        and "deckblatt" not in os.path.basename(p).lower()
    ]


def download_pdfs_for_applicant(browser, download_dir, extract_dir, applicant_num):
    print(f"erro: Download-Button {applicant_num}")

    xpaths = [
        "//button[contains(@aria-label,'Nachweise herunterladen')]",
        "//button[contains(@title,'Nachweise herunterladen')]",
        "//button[.//img[contains(@src,'download.svg')]]",
        "//img[@alt='Nachweise herunterladen']/ancestor::button",
    ]

    dl_element = None
    for xp in xpaths:
        try:
            dl_element = WebDriverWait(browser, 3).until(
                EC.element_to_be_clickable((By.XPATH, xp))
            )
            if dl_element:
                print(f"DEBUG: Button gefunden: {xp}")
                break
        except Exception:
            pass

    if not dl_element:
        print(f"Kein Download-Element{applicant_num} ")
        return []

    for f in glob.glob(os.path.join(download_dir, "*")):
        try:
            if os.path.isfile(f) and f.lower().endswith((".zip", ".pdf")):
                print(f"DEBUG: Removing leftover file in download_dir: {f}")
                os.remove(f)
        except Exception as e:
            print(f"erro {f} couldnt be deleted {e}")

    print("erro: click Download-Butto")
    try:
        browser.execute_script("arguments[0].click();", dl_element)
    except:
        try:
            dl_element.click()
        except Exception as e:
            print(f"error  {e}")

    prev_zips = glob.glob(os.path.join(download_dir, "*.zip"))

    zip_path = wait_for_any_file(
        download_dir,
        pattern="*.zip",
        timeout=40,
        prev=prev_zips,
    )

    if not zip_path:
        print(f"error : no zip for {applicant_num} ")
        return []

    extract_target = os.path.join(
        extract_dir, f"{applicant_num}_{int(time.time())}")
    print(f"error unpacking zip {extract_target}")
    extract_zip_to_dir(zip_path, extract_target)

    pdfs = find_pdfs_in_dir(extract_target)
    print(
        f"DEBUG: {len(pdfs)} PDFs found: {[os.path.basename(p) for p in pdfs]}")
    return pdfs
