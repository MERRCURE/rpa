import time
import json
import os
import sys
import argparse
import importlib
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from utils.browserautomation import BrowserAutomation
from phases.filterphase_evaluierung import run_filterphase_evaluierung


# 1 URL
FLOW_URL = "https://digstu.hhu.de/qisserver/pages/startFlow.xhtml?_flowId=searchApplicants-flow&navigationPosition=hisinoneapp,applicationEditorGeneratedJSFDtos&recordRequest=true"


def create_chrome_options(download_dir):
    chrome_options = Options()

    chrome_options.add_argument("--window-size=1400,900")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--enable-javascript")

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
    print(" Login filled")

    try:
        wait = WebDriverWait(bot.browser, 15)
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

        print("button clicked")
    except Exception as e:
        print(f"login failed{e}")
        return False

    try:

        WebDriverWait(bot.browser, 15).until(
            lambda d: "startFlow" in d.current_url or "portal" in d.current_url
        )
        print("login succesfull plus redirect")
        return True
    except Exception:
        print("erro:popup active")
        return False


def open_flow(bot):
    print("open page")
    bot.open_url(FLOW_URL)
    WebDriverWait(bot.browser, 15).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    print("site found")


if __name__ == "__main__":

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
        print(f"INFO: Konfiguration '{config_name}' erfolgreich geladen")

        try:
            download_dir = config_module.DOWNLOAD_DIR
            os.makedirs(download_dir, exist_ok=True)
            print(f"INFO: Download secured under : {download_dir}")
        except AttributeError:
            print(f"FATAL: 'DOWNLOAD_DIR'not found '{config_name}.py' ")
            sys.exit(1)

    except ImportError:
        print(
            f"error: config '{config_name}.py' not found ")
        sys.exit(1)

    args = parser.parse_args()
    credentials_path = os.path.join(
        os.path.dirname(__file__), "credentials.json")
    with open("credentials.json", "r", encoding="utf-8-sig") as f:
        credentials = json.load(f)

    username = credentials["username"]
    password = credentials["password"]

    chrome_options = create_chrome_options(download_dir)
    bot = BrowserAutomation(options=chrome_options)

    # 1 url
    login_url = "https://digstu.hhu.de/qisserver/pages/cs/sys/portal/hisinoneStartPage.faces"
    print("STATUS open ")
    bot.open_url(login_url)
    print("STATUS: ready")

    perform_login(bot, username, password)

    print("Popup (7 Sekunden")
    # 2 entfernen
    time.sleep(2)

    open_flow(bot)
    run_filterphase_evaluierung(bot, FLOW_URL, config_module)

    print("STATUS: DONE")
    input("ENTER = finish ")
