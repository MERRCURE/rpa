import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


NOTE_STRICT_RE = re.compile(r"\b([0-6][.,]\d+)\b")
NOTE_WEAK_RE = re.compile(r"(\d+(?:[.,]\d+)?)")

def _floatcast(s):
    if not s: return None
    return float(s.replace(",", "."))

def extract_claimed_from_dom(browser, config):
    claimed = _extract_claimed(browser, config)
    bachelor_country_raw = extract_bachelor_country_from_dom(browser)
    claimed["bachelor_country_raw"] = bachelor_country_raw
    claimed["bachelor_country"] = bachelor_country_raw
    return claimed

def _extract_claimed(browser, config):
    categories = list(getattr(config, "REQUIREMENTS", {}).keys())
    dom_map = getattr(config, "DOM_ECTS_MAP", {})
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
            m = NOTE_STRICT_RE.search(el.text.strip())
            if m:
                claimed["note"] = _floatcast(m.group(1))
    except Exception:
        pass

    if claimed["note"] is None:
        fallback_paths = [
            "//label[contains(normalize-space(.),'Bisherige Durchschnitt')]/following-sibling::div[1]//span",
        ]
        for xp in fallback_paths:
            try:
                el = WebDriverWait(browser, 1).until(
                    EC.presence_of_element_located((By.XPATH, xp))
                )
                m = NOTE_STRICT_RE.search(el.text.strip())
                if m:
                    claimed["note"] = _floatcast(m.group(1))
                    break
            except Exception:
                pass

    try:
        labels = browser.find_elements(
            By.XPATH,
            "//label[contains(normalize-space(.),'CP im Bereich')]"
        )
        for lab in labels:
            t = lab.text.strip().lower()
            cat_found = None

            for dom_key, mapped_cat in dom_map.items():
                if dom_key.lower() in t:
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
            m = NOTE_WEAK_RE.search(sib.text.strip())
            if m:
                claimed[cat_found] += _floatcast(m.group(1))
    except Exception:
        pass

    return claimed


def extract_bachelor_country_from_dom(browser) -> str:
    """
    Reads the DOM field:
      '
    """
    try:
        label = WebDriverWait(browser, 1).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//label[normalize-space(.)="
                    "'Land des Bachelorstudiums (oder eines äquivalenten Abschlusses)']"
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
