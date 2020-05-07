import os

import pandas as pd

from common import list_dir, stem_law_name, create_html_soup, create_soup
from statics import (
    DE_ORIGINAL_PATH_OUTDATED,
    DE_LAW_NAMES_PATH,
    DE_LAW_VALIDITIES_PATH,
    DE_XML_PATH,
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

    df = pd.DataFrame(result)
    df.to_csv(DE_LAW_NAMES_PATH, index=False)
