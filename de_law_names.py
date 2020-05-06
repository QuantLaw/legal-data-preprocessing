import os

import pandas as pd

from common import list_dir, stem_law_name, create_html_soup
from statics import DE_ORIGINAL_PATH, DE_LAW_NAMES_PATH, DE_LAW_VALIDITIES_PATH


def de_law_names_prepare(overwrite):
    """
    Returns a list of newest version of each law
    """
    files = list_dir(DE_ORIGINAL_PATH, ".html")
    validity_table = pd.read_csv(DE_LAW_VALIDITIES_PATH)
    return files, validity_table


def de_law_names(filename, validity_table):

    current_law_id = os.path.splitext(filename)[0].split("_")[1]
    soup = create_html_soup(f"{DE_ORIGINAL_PATH}/{filename}")
    title_section = soup.find("div", {"class": "docLayoutTitel"})
    result = []
    if title_section:
        for tag in title_section.find_all("div", {"class": "doc"}):
            for tag_to_extract in tag.find_all(["fnr", "sup"]):
                tag_to_extract.extract()
            law_name = stem_law_name(tag.get_text())

            # if law_name in result:
            #     if len(result[law_name]) < len(law_name):
            #         # If conflict between mappings map law_name to shortest abk, considering this the most general law
            #         continue

            result.append(
                dict(citename=law_name, citekey=current_law_id, filename=filename)
            )
    return result


def de_law_names_finish(names_per_file):
    print("check 1", len(names_per_file))
    result = []
    for names_of_file in names_per_file:
        result.extend(names_of_file)
    print("check 2", len(result))

    df = pd.DataFrame(result)
    df.to_csv(DE_LAW_NAMES_PATH, index=False)
