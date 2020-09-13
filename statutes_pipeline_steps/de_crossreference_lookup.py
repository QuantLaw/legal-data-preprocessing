import pandas as pd

from utils.common import (
    create_soup,
    ensure_exists,
    get_snapshot_law_list,
    load_law_names,
)
from statics import (
    DE_CROSSREFERENCE_LOOKUP_PATH,
    DE_REFERENCE_PARSED_PATH,
    DE_RVO_REFERENCE_PARSED_PATH,
    DE_RVO_CROSSREFERENCE_LOOKUP_PATH,
)


def de_crossreference_lookup_prepare(overwrite, snapshots, regulations):
    ensure_exists(
        DE_RVO_CROSSREFERENCE_LOOKUP_PATH
        if regulations
        else DE_CROSSREFERENCE_LOOKUP_PATH
    )
    files = []
    law_names_data = load_law_names(regulations)
    for snapshot in snapshots:
        files.append((snapshot, get_snapshot_law_list(snapshot, law_names_data)))
    return files


def de_crossreference_lookup(args, regulations):
    date, files = args
    data = []
    source_folder = (
        DE_RVO_REFERENCE_PARSED_PATH if regulations else DE_REFERENCE_PARSED_PATH
    )
    target_folder = (
        DE_RVO_CROSSREFERENCE_LOOKUP_PATH
        if regulations
        else DE_CROSSREFERENCE_LOOKUP_PATH
    )
    for file in files:

        soup = create_soup(f"{source_folder}/{file}")
        for tag in soup.find_all(citekey=True):
            data.append([tag.attrs["key"], tag.attrs["citekey"]])
    df = pd.DataFrame(data, columns=["key", "citekey"])
    destination_file = f"{target_folder}/{date}.csv"
    df.to_csv(destination_file, index=False)
