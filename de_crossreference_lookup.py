import pandas as pd

from common import create_soup, ensure_exists
from statics import DE_XML_NESTED_PATH, DE_CROSSREFERENCE_LOOKUP_PATH


def de_crossreference_lookup_prepare(overwrite, snapshots):
    ensure_exists(DE_CROSSREFERENCE_LOOKUP_PATH)
    files = []
    for snapshot in snapshots:
        files.append((snapshot, get_snapshot_law_list(snapshot)))
    return files


def de_crossreference_lookup(args):
    date, files = args
    data = []
    for file in files:
        soup = create_soup(f"{DE_XML_NESTED_PATH}/{file}")
        for tag in soup.find_all(citekey=True):
            data.append([tag.attrs["key"], tag.attrs["citekey"]])
    df = pd.DataFrame(data, columns=["key", "citekey"])
    destination_file = f"{DE_CROSSREFERENCE_LOOKUP_PATH}/{date}.csv"
    df.to_csv(destination_file, index=False)
