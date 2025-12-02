
import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RES_DIR = os.path.join(BASE_DIR, "ressources")
DOWNLOAD_DIR = os.path.join(RES_DIR, "downloads")
EXTRACT_DIR = os.path.join(DOWNLOAD_DIR, "extracted")
OUTPUT_DIR = RES_DIR

MODULE_MAP_CSV = os.path.join(RES_DIR, "mathemodule_info.csv")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "bewerber_evaluierung_ai.csv")

DOM_ECTS_MAP = {
    "mathematik": "Mathematik",
    "mathe": "Mathematik"


}


REQ_NOTE_MAX = 2.4
REQUIREMENTS = {
    "Mathematik": 30.0
}
WHITELIST_UNIS = os.path.join(RES_DIR, "whitelist_unis_ai.csv")
