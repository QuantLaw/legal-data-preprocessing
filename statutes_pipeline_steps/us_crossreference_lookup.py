import os

import pandas as pd

from utils.common import list_dir, ensure_exists, create_soup

from statics import US_REFERENCE_PARSED_PATH, US_CROSSREFERENCE_LOOKUP_PATH


def get_filename(year):
    return f"{year}.csv"


def us_crossreference_lookup_prepare(overwrite, snapshots):
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


def us_crossreference_lookup(year):
    yearfiles = [
        x for x in list_dir(US_REFERENCE_PARSED_PATH, ".xml") if str(year) in x
    ]
    data = []
    for file in yearfiles:
        soup = create_soup(f"{US_REFERENCE_PARSED_PATH}/{file}")
        for tag in soup.find_all(citekey=True):
            data.append([tag.attrs["key"], tag.attrs["citekey"]])
    df = pd.DataFrame(data, columns=["key", "citekey"])
    destination_file = f"{US_CROSSREFERENCE_LOOKUP_PATH}/{get_filename(year)}"
    df.to_csv(destination_file, index=False)
