import os
import pickle

import pandas as pd

from common import list_dir, stem_law_name, create_html_soup, create_soup
from statics import (
    DE_ORIGINAL_PATH_OUTDATED,
    DE_LAW_NAMES_PATH,
    DE_LAW_VALIDITIES_PATH,
    DE_XML_PATH,
    DE_LAW_NAMES_COMPILED_PATH,
)


def de_law_names_prepare(overwrite):
    files = list_dir(DE_XML_PATH, ".xml")
    return files


def de_law_names(filename):

    soup = create_soup(f"{DE_XML_PATH}/{filename}")
    document = soup.find("document", recursive=False)
    result = set()
    citekey = document.attrs["key"].split("_")[1]

    if "heading" in document.attrs:
        law_name = stem_law_name(document.attrs["heading"])
        result.add((law_name, citekey, filename))

    if "abk_1" in document.attrs:
        law_name = stem_law_name(document.attrs["abk_1"])
        result.add((law_name, citekey, filename))

    if "abk_2" in document.attrs:
        law_name = stem_law_name(document.attrs["abk_2"])
        result.add((law_name, citekey, filename))
    return result


def de_law_names_finish(names_per_file):
    result = []
    for names_of_file in names_per_file:
        result.extend(names_of_file)

    df = pd.DataFrame(result, columns=["citename", "citekey", "filename"])
    df.to_csv(DE_LAW_NAMES_PATH, index=False)

    dated_law_names = compile_law_names()
    with open(DE_LAW_NAMES_COMPILED_PATH, "wb") as f:
        pickle.dump(dated_law_names, f)


def compile_law_names():
    df = pd.read_csv(DE_LAW_NAMES_PATH)
    data = [
        dict(
            citename=row.citename,
            citekey=row.citekey,
            start=row.filename.split("_")[1],
            end=os.path.splitext(row.filename)[0].split("_")[2],
        )
        for i, row in df.iterrows()
    ]
    dates = sorted({r["start"] for r in data})
    df = None

    dated_law_names = {}

    date_len = len(dates)
    for i, date in enumerate(dates):
        if i % 100 == 0:
            print(f"\r{i/date_len}", end="")
        law_names_list = [d for d in data if d["start"] <= date and d["end"] >= date]
        law_names = {}
        for row in law_names_list:
            law_names[row["citename"]] = row["citekey"]
        dated_law_names[date] = law_names
    print()

    return dated_law_names
