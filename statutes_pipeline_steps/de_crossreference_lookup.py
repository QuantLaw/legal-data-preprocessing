import pandas as pd
from quantlaw.utils.files import ensure_exists

from utils.common import (
    get_snapshot_law_list,
    load_law_names,
)
from statics import DE_CROSSREFERENCE_LOOKUP_PATH, DE_REFERENCE_PARSED_PATH


def de_crossreference_lookup_prepare(overwrite, snapshots):
    ensure_exists(DE_CROSSREFERENCE_LOOKUP_PATH)
    files = []
    law_names_data = load_law_names()
    for snapshot in snapshots:
        files.append((snapshot, get_snapshot_law_list(snapshot, law_names_data)))
    return files


def de_crossreference_lookup(args):
    date, files = args
    data = []
    for file in files:
        soup = create_soup(f"{DE_REFERENCE_PARSED_PATH}/{file}")
        for tag in soup.find_all(citekey=True):
            data.append([tag.attrs["key"], tag.attrs["citekey"]])
    df = pd.DataFrame(data, columns=["key", "citekey"])
    destination_file = f"{DE_CROSSREFERENCE_LOOKUP_PATH}/{date}.csv"
    df.to_csv(destination_file, index=False)
