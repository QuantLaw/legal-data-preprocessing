import os

import pandas as pd
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.pipeline import PipelineStep

from statics import US_CROSSREFERENCE_LOOKUP_PATH, US_REFERENCE_PARSED_PATH


class UsCrossreferenceLookup(PipelineStep):
    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(US_CROSSREFERENCE_LOOKUP_PATH)

        # If snapshots not set, create list of all years
        if not snapshots:
            snapshots = sorted(
                set(
                    [
                        x.split(".")[0].split("_")[-1]
                        for x in list_dir(US_REFERENCE_PARSED_PATH, ".xml")
                    ]
                )
            )

        if not overwrite:
            existing_files = os.listdir(US_CROSSREFERENCE_LOOKUP_PATH)
            snapshots = list(
                filter(lambda f: get_filename(f) not in existing_files, snapshots)
            )

        return snapshots

    def execute_item(self, item):
        yearfiles = [
            x for x in list_dir(US_REFERENCE_PARSED_PATH, ".xml") if str(item) in x
        ]
        data = []
        for file in yearfiles:
            soup = create_soup(f"{US_REFERENCE_PARSED_PATH}/{file}")
            for tag in soup.find_all(citekey=True):
                data.append([tag.attrs["key"], tag.attrs["citekey"]])
        df = pd.DataFrame(data, columns=["key", "citekey"])
        destination_file = f"{US_CROSSREFERENCE_LOOKUP_PATH}/{get_filename(item)}"
        df.to_csv(destination_file, index=False)


def get_filename(year):
    return f"{year}.csv"
