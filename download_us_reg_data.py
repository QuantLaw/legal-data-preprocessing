import re
import shutil
from multiprocessing.pool import Pool
from zipfile import ZipFile

import lxml.etree
import requests
from bs4 import BeautifulSoup

from statics import US_REG_INPUT_PATH
from utils.common import ensure_exists

DOWNLOAD_BASE_URL = "https://www.govinfo.gov/bulkdata/CFR/{}/CFR-{}.zip"


def download(year):
    print("loading", year)
    r = requests.get(DOWNLOAD_BASE_URL.format(year, year), stream=True)
    if r.status_code == 200:
        zip_path = f"{US_REG_INPUT_PATH}/{year}.zip"
        with open(zip_path, "wb") as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
        print("downloaded", year)

        with ZipFile(zip_path) as f:
            f.extractall(ensure_exists(f"{US_REG_INPUT_PATH}/{year}"))


if __name__ == "__main__":

    ensure_exists(US_REG_INPUT_PATH)
    with Pool(4) as p:
        p.map(download, list(range(1996, 2020 + 1)))
