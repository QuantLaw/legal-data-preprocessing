import json
import os

import numpy
import pandas as pd

from utils.common import (
    ensure_exists,
    create_soup,
    get_snapshot_law_list,
)
from statics import (
    DE_CROSSREFERENCE_EDGELIST_PATH,
    DE_CROSSREFERENCE_LOOKUP_PATH,
    DE_REFERENCE_PARSED_PATH,
)


def get_filename(date):
    return f"{date}.csv"


def de_crossreference_edgelist_prepare(overwrite, snapshots):
    ensure_exists(DE_CROSSREFERENCE_EDGELIST_PATH)

    if not overwrite:
        existing_files = os.listdir(DE_CROSSREFERENCE_EDGELIST_PATH)
        snapshots = list(
            filter(lambda f: get_filename(f) not in existing_files, snapshots)
        )

    return snapshots


def de_crossreference_edgelist(snapshot, law_names_data):
    files = get_snapshot_law_list(snapshot, law_names_data)
    key_df = (
        pd.read_csv(f"{DE_CROSSREFERENCE_LOOKUP_PATH}/{snapshot}.csv")
        .dropna()
        .set_index("citekey")
    )
    df = None
    for file in files:
        edge_df = make_edge_list(file, key_df)
        df = edge_df if df is None else df.append(edge_df, ignore_index=True)
    df.to_csv(f"{DE_CROSSREFERENCE_EDGELIST_PATH}/{snapshot}.csv", index=False)


def make_edge_list(file, key_df):
    soup = create_soup(f"{DE_REFERENCE_PARSED_PATH}/{file}")
    edges = []

    # FOR DEBUG
    problem_matches = set()
    problem_keys = set()

    for item in soup.find_all("seqitem"):
        references = item.find_all("reference")
        if references:
            node_out = item.get("key")
            for node in references:
                if node.lawname and node.lawname.get("type") in [
                    "dict",
                    "sgb",
                    "internal",
                ]:
                    refs = json.loads(node.attrs["parsed"])
                    for ref in refs:
                        try:
                            key = "_".join(ref[:2])
                            matches = key_df.at[key, "key"]
                            if type(matches) == numpy.ndarray:
                                print(f"Multiple matches for {key}")
                                matches = matches[0]
                            if type(matches) is not str:
                                problem_matches.add(tuple(matches))
                            node_in = matches if type(matches) == str else matches[0]
                            edges.append((node_out, node_in))
                            assert len(ref) > 1
                        except KeyError:
                            problem_keys.add(key)

    # FOR DEBUG
    # if len(problem_matches) > 0:
    #     print(f"{file} Problem Matches:\n", sorted(list(problem_matches)))
    # if len(problem_keys) > 0:
    #     print(f"{file} Problem Matches:\n", sorted(list(problem_keys)))
    return pd.DataFrame(edges, columns=["out_node", "in_node"])
