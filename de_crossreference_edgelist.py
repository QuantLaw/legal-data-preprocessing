import json
import os

import numpy
import pandas as pd

from common import ensure_exists, create_soup, get_snapshot_law_list
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


def de_crossreference_edgelist(snapshot):
    files = get_snapshot_law_list(snapshot)
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
    edge_df = pd.DataFrame(columns=["out_node", "in_node"])

    # FOR DEBUG
    problem_matches = set()
    problem_keys = set()

    for item in soup.find_all("seqitem"):
        if item.find_all("reference"):
            node_out = item.get("key")
            for node in item.find_all("reference"):
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
                            # TODO LATER DEBUG - the way the lookup key is composed is likely a source of errors
                            if type(matches) is not str:
                                problem_matches.add(tuple(matches))
                            node_in = matches if type(matches) == str else matches[0]
                            edge_df = edge_df.append( # TODO improve performance
                                pd.DataFrame(
                                    dict(in_node=[node_in], out_node=[node_out])
                                ),
                                ignore_index=True,
                            )
                            assert len(ref) > 1
                        except KeyError:
                            problem_keys.add(key)

    # FOR DEBUG TODO LATER REMOVE
    if len(problem_matches) > 0:
        print(f"{file} Problem Matches:\n", sorted(list(problem_matches)))
    if len(problem_keys) > 0:
        print(f"{file} Problem Matches:\n", sorted(list(problem_keys)))
    return edge_df[["out_node", "in_node"]]
