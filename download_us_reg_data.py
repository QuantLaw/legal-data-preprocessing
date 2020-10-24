import os
import shutil
from multiprocessing.pool import Pool

import requests

from statics import US_REG_INPUT_PATH


def ensure_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


DOWNLOAD_BASE_URL = "https://www.govinfo.gov/bulkdata/CFR/{}/CFR-{}.zip"


def download(year):
    zip_path = f"{US_REG_INPUT_PATH}/{year}.zip"
    if not os.path.exists(zip_path):
        print("loading", year)
        r = requests.get(DOWNLOAD_BASE_URL.format(year, year), stream=True)
        if r.status_code == 200:
            with open(zip_path, "wb") as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
            print("downloaded", year)


if __name__ == "__main__":

    ensure_exists(US_REG_INPUT_PATH)
    with Pool(4) as p:
        p.map(download, list(range(1996, 2020 + 1)))
