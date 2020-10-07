import json
import os

import pandas as pd
from quantlaw.utils.files import list_dir

from statics import (
    US_REFERENCE_PARSED_PATH,
    US_CROSSREFERENCE_LOOKUP_PATH,
    US_CROSSREFERENCE_EDGELIST_PATH,
)


def get_filename(date):
    return f"{date}.csv"


def us_crossreference_edgelist_prepare(overwrite, snapshots):
    ensure_exists(US_CROSSREFERENCE_EDGELIST_PATH)
    if not snapshots:
        snapshots = sorted(
            set(
                [
                    os.path.splitext(x)[0]
                    for x in list_dir(US_CROSSREFERENCE_LOOKUP_PATH, ".csv")
                ]
            )
        )

    if not overwrite:
        existing_files = os.listdir(US_CROSSREFERENCE_EDGELIST_PATH)
        snapshots = list(
            filter(lambda f: get_filename(f) not in existing_files, snapshots)
        )

    return snapshots


def us_crossreference_edgelist(year):
    yearfiles = [
        x for x in list_dir(US_REFERENCE_PARSED_PATH, ".xml") if str(year) in x
    ]
    key_df = (
        pd.read_csv(f"{US_CROSSREFERENCE_LOOKUP_PATH}/{year}.csv")
        .dropna()
        .set_index("citekey")
    )
    df = None
    for yearfile_path in yearfiles:
        edge_df = make_edge_list(yearfile_path, key_df)
        df = edge_df if df is None else df.append(edge_df, ignore_index=True)
    df.to_csv(f"{US_CROSSREFERENCE_EDGELIST_PATH}/{year}.csv", index=False)


def make_edge_list(yearfile_path, key_df):
    soup = create_soup(f"{US_REFERENCE_PARSED_PATH}/{yearfile_path}")
    edge_df = pd.DataFrame(columns=["out_node", "in_node"])

    # FOR DEBUG
    problem_matches = set()
    problem_keys = set()

    for item in soup.find_all(["seqitem"]):
        if item.find_all(["reference"]):
            node_out = item.get("key")
            for node in item.find_all(["reference"]):
                refs = json.loads(node.attrs["parsed"])
                for ref in refs:
                    try:
                        key = "_".join(ref[:2])
                        matches = key_df.at[key, "key"]
                        if type(matches) != str:
                            problem_matches.add(tuple(matches))
                        node_in = matches if type(matches) == str else matches[0]
                        edge_df = edge_df.append(
                            pd.DataFrame(dict(in_node=[node_in], out_node=[node_out])),
                            ignore_index=True,
                        )
                        assert len(ref) > 1
                    except KeyError:
                        problem_keys.add(key)

    # if len(problem_matches) > 0:
    #     print(f"{yearfile_path} Problem Matches:\n", sorted(list(problem_matches)))
    # if len(problem_keys) > 0:
    #     print(f"{yearfile_path} Problem Matches:\n", sorted(list(problem_keys)))
    return edge_df[["out_node", "in_node"]]
