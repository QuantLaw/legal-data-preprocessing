import pickle

import pandas as pd
from quantlaw.de_extract.stemming import stem_law_name
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import list_dir
from quantlaw.utils.pipeline import PipelineStep

from statics import DE_LAW_NAMES_COMPILED_PATH, DE_LAW_NAMES_PATH, DE_XML_PATH
from utils.common import load_law_names
from statics import (
    DE_LAW_NAMES_COMPILED_PATH,
    DE_LAW_NAMES_PATH,
    DE_RVO_LAW_NAMES_COMPILED_PATH,
    DE_RVO_LAW_NAMES_PATH,
    DE_RVO_XML_PATH,
    DE_XML_PATH,
)
from utils.common import load_law_names


class DeLawNamesStep(PipelineStep):
    def get_items(self) -> list:
        src = DE_RVO_XML_PATH if regulations else DE_XML_PATH
        files = list_dir(src, ".xml")
        return files

    def execute_item(self, item):
        src = DE_RVO_XML_PATH if regulations else DE_XML_PATH
        soup = create_soup(f"{src}/{item}")
        document = soup.find("document", recursive=False)
        result = set()
        citekey = document.attrs["key"].split("_")[1]

        if "heading" in document.attrs:
            law_name = stem_law_name(document.attrs["heading"])
            result.add((law_name, citekey, item))

        if "heading_short" in document.attrs:
            law_name = stem_law_name(document.attrs["heading_short"])
            result.add((law_name, citekey, item))

        if "abbr_1" in document.attrs:
            law_name = stem_law_name(document.attrs["abbr_1"])
            result.add((law_name, citekey, item))

        if "abbr_2" in document.attrs:
            law_name = stem_law_name(document.attrs["abbr_2"])
            result.add((law_name, citekey, item))
        return result

    def finish_execution(self, names_per_file, regulations):
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
