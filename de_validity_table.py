import itertools
import os
import re

import pandas as pd

from common import list_dir, create_html_soup, create_soup, find_parent_with_name
from statics import (
    DE_ORIGINAL_VERSION_INDICES_PATH,
    DE_LAW_VALIDITIES_PATH,
    DE_ORIGINAL_PATH,
    DE_XML_NESTED_PATH,
)


def de_validity_table_prepare(overwrite):
    version_indices = list_dir(DE_ORIGINAL_VERSION_INDICES_PATH, ".html")
    xmls = list_dir(DE_XML_NESTED_PATH, ".xml")
    xmls_dict = {
        (
            os.path.splitext(xml)[0].split("_")[0],
            os.path.splitext(xml)[0].split("_")[-1],
        ): xml
        for xml in xmls
    }
    return version_indices, xmls_dict


def de_validity_table(filename, xmls_dict):
    file_validities = []
    soup = create_html_soup(f"{DE_ORIGINAL_VERSION_INDICES_PATH}/{filename}")
    try:
        links = [
            (
                re.search(
                    r"(?:/document/|\?docId=)(?:aiz\-)?([\w\-\@]+?)(?:/|&)",
                    x.attrs["href"].replace("%40", "@"),
                )[1],
                x.get_text(),
            )  # TODO UPDATE to /document/ for new laws
            for x in soup.find("article").find_all("a")
            if x.get("href")
        ]
        links = [link for link in links if link[1]]  # Filter invisible links
    except TypeError:
        print(filename)
        raise
    for link in links:
        match = re.fullmatch(
            r".+\sgültig\s(?:ab|von):\s(\d{2}\.\d{2}\.\d{4})(?:\s.*gültig\sbis:\s(\d{2}\.\d{2}\.\d{4}))?",
            link[1],
            flags=re.IGNORECASE,
        )
        if not match:
            # print(f"\nError: {link} in {filename}")
            print(f"Link(s) not matched in: {filename}")
            break
        start = convert_date(match[1])
        end = convert_date(match[2])
        nr = link[0].split("@")[0]
        version = link[0].split("@")[-1] if "@" in link[0] else "99999999"
        if (nr, version) in xmls_dict:
            filename = xmls_dict[(nr, version)]
            file_validities.append((filename, start, end))
    return file_validities


def de_validity_table_finish(data, xmls_dict):
    file_validities = list(itertools.chain.from_iterable(data))
    append_single_version_files(file_validities, xmls_dict)
    df = pd.DataFrame(file_validities, columns=["filename", "start", "end"])
    df.to_csv(DE_LAW_VALIDITIES_PATH)


def analyze_single_version_file(filename):
    soup = create_soup(f"{DE_ORIGINAL_PATH}/{filename.replace('.xml', '.html')}")
    document_header = soup.body.find("div", {"class": "documentHeader"})
    valid_label = document_header.find(text=re.compile("G.ltig\s*ab.:?"))
    try:
        tr = find_parent_with_name(valid_label, "tr")
    except AttributeError:
        print(filename)
        raise
    valid_text = tr.find("td", {"class": "TD70"}).get_text()
    date_match = re.fullmatch(r"(\d\d)\.(\d\d)\.(\d{4})", valid_text)
    assert date_match
    start = convert_date(valid_text)
    return (filename, start, None)


def append_single_version_files(file_validities, xmls_dict):
    processed_files = set([x[0] for x in file_validities])
    all_files = set(xmls_dict.values())
    single_version_files = all_files - processed_files
    for file in single_version_files:
        file_validities.append(analyze_single_version_file(file))


def convert_date(text):
    return "-".join(reversed(text.split("."))) if text else None
