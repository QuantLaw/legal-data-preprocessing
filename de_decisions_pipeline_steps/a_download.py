import os
import zipfile
from multiprocessing.pool import Pool

import requests
from bs4 import BeautifulSoup

from statics import (
    DE_DECISIONS_DOWNLOAD_TOC,
    DE_DECISIONS_DOWNLOAD_XML,
    DE_DECISIONS_DOWNLOAD_ZIP,
    DE_DECISIONS_TEMP_DATA_PATH,
)
from utils.common import ensure_exists


def download_item(link_text):
    filename = link_text.split("/")[-1]
    if not os.path.isfile(f"{DE_DECISIONS_DOWNLOAD_ZIP}/{filename}"):
        content = requests.get(link_text).content
        with open(f"{DE_DECISIONS_DOWNLOAD_ZIP}/{filename}", "wb") as f:
            f.write(content)


def download():
    ensure_exists(DE_DECISIONS_TEMP_DATA_PATH)
    toc = requests.get("https://www.rechtsprechung-im-internet.de/rii-toc.xml").text
    with open(DE_DECISIONS_DOWNLOAD_TOC, "w") as f:
        f.write(toc)

    with open(DE_DECISIONS_DOWNLOAD_TOC) as f:
        toc = f.read()
    soup = BeautifulSoup(toc, "lxml-xml")
    len(soup.findAll("item"))

    ensure_exists(DE_DECISIONS_DOWNLOAD_ZIP)
    items = [i.link.text for i in soup.findAll("item")]
    with Pool(4) as p:
        p.map(download_item, items)

    ensure_exists(DE_DECISIONS_DOWNLOAD_XML)

    i = 0
    for filename in os.listdir(DE_DECISIONS_DOWNLOAD_ZIP):
        if os.path.splitext(filename)[1] == ".zip":
            zip_ref = zipfile.ZipFile(f"{DE_DECISIONS_DOWNLOAD_ZIP}/{filename}", "r")
            zip_ref.extractall(DE_DECISIONS_DOWNLOAD_XML)
            zip_ref.close()
            i += 1
            print(f"\r{i} entpackt", end="")
