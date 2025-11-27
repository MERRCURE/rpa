import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

NOTE_STRICT_RE = re.compile(r"\b([0-6][.,]\d+)\b")

def extract_claimed_from_dom(browser, config):
    categories = list(getattr(config, "REQUIREMENTS", {}).keys())
    dom_map = getattr(config, "DOM_ECTS_MAP", {})
    claimed = extract_claimed(browser, categories, dom_map)

    bachelor_country_raw = extract_bachelor_country_from_dom(browser)
    claimed["bachelor_country_raw"] = bachelor_country_raw
    claimed["bachelor_country"] = bachelor_country_raw

    return claimed

def extract_claimed(browser, categories, dom_map):
    dom_map = {k.lower(): v for k, v in dom_map.items()}

    claimed = {"note": None}
    for c in categories:
        claimed[c] = 0.0

    try:
        label = WebDriverWait(browser, 1).until(
            EC.presence_of_element_located(
                (By.XPATH, "//label[normalize-space(.)='Ergebnis MZB-Note']")
            )
        )
        nid = label.get_attribute("for")
        if nid:
            el = browser.find_element(By.XPATH, f"//div[@id='{nid}']//span")
            txt = el.text.strip()
            m = NOTE_STRICT_RE.search(txt) or NOTE_STRICT_RE.search(txt)
            if m:
                claimed["note"] = float(m.group(1).replace(",", "."))
    except Exception:
        pass

    if claimed["note"] is None:
        fallback_paths = [
            "//label[contains(normalize-space(.),'Bisherige Durchschnitt')]/following-sibling::div[1]//span"
        ]
        for xp in fallback_paths:
            try:
                el = WebDriverWait(browser, 1).until(
                    EC.presence_of_element_located((By.XPATH, xp))
                )
                txt = el.text.strip()
                m = NOTE_STRICT_RE.search(txt) or NOTE_STRICT_RE.search(txt)
                if m:
                    claimed["note"] = float(m.group(1).replace(",", "."))
                    break
            except Exception:
                pass

    try:
        labels = browser.find_elements(
            By.XPATH,
            "//label[contains(.,'im Bereich') and contains(.,'CP')]"
        )
        for lab in labels:
            t = lab.text.strip().lower()
            cat_found = None

            for dom_key, mapped_cat in dom_map.items():
                if dom_key in t:
                    cat_found = mapped_cat
                    break

            if not cat_found:
                for cat in categories:
                    if cat.lower() in t:
                        cat_found = cat
                        break

            if not cat_found:
                continue

            sib = lab.find_element(By.XPATH, "following-sibling::*[1]")
            txt = sib.text.strip()
            m = re.search(r"(\d+(?:[.,]\d+)?)", txt)
            if m:
                claimed[cat_found] += float(m.group(1).replace(",", "."))
    except Exception:
        pass

    return claimed

def extract_bachelor_country_from_dom(browser):
    try:
        label = WebDriverWait(browser, 1).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//label[normalize-space(.)='Land des Bachelorstudiums (oder eines äquivalenten Abschlusses)']"
                )
            )
        )
        val_el = label.find_element(
            By.XPATH,
            "./following-sibling::div[1]//span"
        )
        return val_el.text.strip()
    except Exception:
        return ""

def get_university_from_dom(browser):
    try:
        label = WebDriverWait(browser, 1).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 "//label[contains(normalize-space(.),'Name der Hochschule')]")
            )
        )
        v = label.find_element(
            By.XPATH,
            "./following-sibling::div[1]//span"
        ).text.strip()
        return v
    except Exception:
        return ""