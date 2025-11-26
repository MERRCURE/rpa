import time
import json
import os
import sys
import argparse
import importlib
import sys
import logging
logging.basicConfig(
    level=logging.INFO, # Change this to logging.INFO to hide debugs
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from utils.browserautomation import BrowserAutomation
from phases.filterphase_evaluierung import run_filterphase_evaluierung



LOGIN_URL = "https://digstu.hhu.de/qisserver/pages/cs/sys/portal/hisinoneStartPage.faces"
FLOW_URL = "https://digstu.hhu.de/qisserver/pages/startFlow.xhtml?_flowId=searchApplicants-flow&navigationPosition=hisinoneapp,applicationEditorGeneratedJSFDtos&recordRequest=true"


def create_chrome_options(download_dir):
    chrome_options = Options()

    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--enable-javascript")
#    chrome_options.add_argument("--window-size=1400,900")
    chrome_options.add_argument("--headless") # prevents focus stealing, can work alongside ;-)

    chrome_options.add_experimental_option("prefs", {
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
    })

    return chrome_options


def perform_login(bot, username, password):
    logging.info("Performing Login...")

    try:
        wait = WebDriverWait(bot.browser, 2)
        user_field = wait.until(
            EC.presence_of_element_located((By.ID, "asdf")))
        pass_field = wait.until(
            EC.presence_of_element_located((By.ID, "fdsa")))
        login_btn = wait.until(
            EC.element_to_be_clickable((By.ID, "loginForm:login")))

        user_field.clear()
        user_field.send_keys(username)
        pass_field.clear()
        pass_field.send_keys(password)
        login_btn.click()

        logging.debug("Clicked on login button")
    except Exception as e:
        logging.error(f"login failed{e}")
        return False

    try:

        WebDriverWait(bot.browser, 2).until(
            lambda d: "startFlow" in d.current_url or "portal" in d.current_url
        )
        logging.info("Login succesfull plus redirect")
        return True
    except Exception:
        logging.error("Could not Login, maybe there is another (.htaccess-based) popup active?")
        return False


def open_flow(bot):
    logging.info("Opening page")
    bot.open_url(FLOW_URL)
    WebDriverWait(bot.browser, 2).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    logging.info("Found site")

def main():
    logging.debug("Obtaining configuration and credentials...")
    ### OBTAIN CONFIG+CREDENTIALS
    parser = argparse.ArgumentParser(
        description="start")
    parser.add_argument(
        "-c", "--config",
        help="Name der zu ladenden Konfigurationsdatei ",
        required=True
    )
    args = parser.parse_args()

    config_name = args.config
    try:

        config_module = importlib.import_module(f"config.{config_name}")
        logging.info(f"Konfiguration '{config_name}' erfolgreich geladen")

        try:
            download_dir = config_module.DOWNLOAD_DIR
            os.makedirs(download_dir, exist_ok=True)
            logging.info(f"Download secured under : {download_dir}")
        except AttributeError:
            logging.critical(f"'DOWNLOAD_DIR' not found '{config_name}.py' ")
            sys.exit(1)

    except ImportError:
        logging.critical(
            f"config '{config_name}.py' not found ")
        sys.exit(1)

    args = parser.parse_args()
    credentials_path = os.path.join(
        os.path.dirname(__file__), "credentials.json")
    with open("credentials.json", "r", encoding="utf-8-sig") as f:
        credentials = json.load(f)

    username = credentials["username"]
    password = credentials["password"]

    ### START BROWSER
    logging.debug("Opening browser...")
    chrome_options = create_chrome_options(download_dir)
    bot = BrowserAutomation(options=chrome_options)

    logging.debug("Opening URL...")
    bot.open_url(LOGIN_URL)
    logging.debug("Logging in...")

    perform_login(bot, username, password)

    logging.debug("Waiting for popup (0.1 second)...")
    time.sleep(0.1)

    logging.debug("Open flow...")
    open_flow(bot)
    logging.debug("Filterphase Evaluierung...")
    run_filterphase_evaluierung(bot, FLOW_URL, config_module)

    logging.info("Done.")
#    input("ENTER = finish ")# why not just terminate the program at the end? makes profiling easier.

if __name__ == "__main__":
    main()
