import pickle

import pandas as pd

from utils.common import (
    list_dir,
    stem_law_name,
    create_soup,
    load_law_names,
)
from statics import (
    DE_LAW_NAMES_PATH,
    DE_XML_PATH,
    DE_LAW_NAMES_COMPILED_PATH,
    DE_RVO_XML_PATH,
    DE_RVO_LAW_NAMES_COMPILED_PATH,
    DE_RVO_LAW_NAMES_PATH,
)


def de_law_names_prepare(overwrite, regulations):
    src = DE_RVO_XML_PATH if regulations else DE_XML_PATH

    files = list_dir(src, ".xml")
    return files


def de_law_names(filename, regulations):
    src = DE_RVO_XML_PATH if regulations else DE_XML_PATH

    soup = create_soup(f"{src}/{filename}")
    document = soup.find("document", recursive=False)
    result = set()
    citekey = document.attrs["key"].split("_")[1]

    if "heading" in document.attrs:
        law_name = stem_law_name(document.attrs["heading"])
        result.add((law_name, citekey, filename))

    if "heading_short" in document.attrs:
        law_name = stem_law_name(document.attrs["heading_short"])
        result.add((law_name, citekey, filename))

    if "abbr_1" in document.attrs:
        law_name = stem_law_name(document.attrs["abbr_1"])
        result.add((law_name, citekey, filename))

    if "abbr_2" in document.attrs:
        law_name = stem_law_name(document.attrs["abbr_2"])
        result.add((law_name, citekey, filename))
    return result


def de_law_names_finish(names_per_file, regulations):
    dest_compiled = (
        DE_RVO_LAW_NAMES_COMPILED_PATH if regulations else DE_LAW_NAMES_COMPILED_PATH
    )
    dest_csv = DE_RVO_LAW_NAMES_PATH if regulations else DE_LAW_NAMES_PATH

    result = []
    for names_of_file in names_per_file:
        result.extend(names_of_file)

    df = pd.DataFrame(result, columns=["citename", "citekey", "filename"])
    df.to_csv(dest_csv, index=False)

    dated_law_names = compile_law_names(regulations)
    with open(dest_compiled, "wb") as f:
        pickle.dump(dated_law_names, f)


def compile_law_names(regulations):
    data = load_law_names(regulations)
    dates = sorted({r["start"] for r in data})

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
