import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class BrowserAutomation:
    def __init__(self, options=None):
        if options:
            self.browser = webdriver.Chrome(options=options)
        else:
            self.browser = webdriver.Chrome()
        self.wait = WebDriverWait(self.browser, 2)

    def open_url(self, url: str):
        self.browser.get(url)

        try:
            WebDriverWait(self.browser, 2).until(
                lambda d: d.execute_script(
                    "return document.readyState") == "complete"
            )
        except Exception:
            logging.error("Timeout: kein readyState=complete")

        #try:
        #    body_visible = WebDriverWait(self.browser, 1).until(
        #        lambda d: d.execute_script(
        #            "return document.body.style.visibility") != "hidden"
        #    )
        #    if body_visible:
        #        logging.debug("Seite sichtbar.")
        #except Exception:
        #    self.browser.execute_script(
        #        "document.body.style.visibility='visible';")
        #    logging.warn("sichtbarkeit manuell (?)")

    def add_input(self, by: By, value: str, text: str):
        """auf DOM praesenz warten"""
        try:
            field = WebDriverWait(self.browser, 2).until(
                EC.presence_of_element_located((by, value))
            )
            time.sleep(0.1)
            self.browser.execute_script(
                "arguments[0].scrollIntoView(true);", field)
            field.clear()
            field.send_keys(text)
            logging.debug(f"Text '{text}' in Feld {value} eingetragen.")
        except Exception as e:
            logging.error(f"Konnte Feld {value} nicht ausfüllen: {e}")

    def click_button(self, by: By, value: str):
        try:
            button = WebDriverWait(self.browser, 1).until(
                EC.element_to_be_clickable((by, value))
            )
            button.click()
            logging.debug(f"Button {value} geklickt.")
        except Exception as e:
            logging.error(f"Button {value} nicht klickbar: {e}")
