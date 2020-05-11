import re
import shutil
from multiprocessing.pool import Pool
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup

from statics import US_INPUT_PATH
from utils.common import ensure_exists

INDEX_URL = (
    "https://uscode.house.gov/download/annualhistoricalarchives/downloadxhtml.shtml"
)

DOWNLOAD_BASE_URL = "https://uscode.house.gov/download/annualhistoricalarchives/"


def download(ref):
    year = re.match(r"XHTML/(\d+)\.zip", ref)[1]
    print("leading", year)
    r = requests.get(DOWNLOAD_BASE_URL + ref, stream=True)
    if r.status_code == 200:
        zip_path = f"{US_INPUT_PATH}/{year}.zip"
        with open(zip_path, "wb") as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)

        with ZipFile(zip_path) as f:
            f.extractall(US_INPUT_PATH)


if __name__ == "__main__":
    response = requests.get(INDEX_URL)
    soup = BeautifulSoup(str(response.content), "lxml")
    refs = []
    for s_string in soup.find_all(text=" zip file]"):
        a_tag = s_string.parent
        assert a_tag.name == "a"
        refs.append(a_tag.attrs["href"])

    ensure_exists(US_INPUT_PATH)

    with Pool(4) as p:
        p.map(download, sorted(refs))
