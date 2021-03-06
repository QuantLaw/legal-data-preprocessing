import json
import os

import numpy
import pandas as pd
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists
from quantlaw.utils.pipeline import PipelineStep

from statics import (
    DE_REFERENCE_PARSED_PATH,
    DE_REG_AUTHORITY_EDGELIST_PATH,
    DE_REG_CROSSREFERENCE_LOOKUP_PATH,
    DE_REG_REFERENCE_PARSED_PATH,
)
from utils.common import get_snapshot_law_list


def get_filename(date):
    return f"{date}.csv"


class DeAuthorityEdgelist(PipelineStep):
    def __init__(self, law_names_data, *args, **kwargs):
        self.law_names_data = law_names_data
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(DE_REG_AUTHORITY_EDGELIST_PATH)

        if not overwrite:
            existing_files = os.listdir(DE_REG_AUTHORITY_EDGELIST_PATH)
            snapshots = list(
                filter(lambda f: get_filename(f) not in existing_files, snapshots)
            )

        return snapshots

    def execute_item(self, item):
        files = get_snapshot_law_list(item, self.law_names_data)
        source_folder = DE_REG_CROSSREFERENCE_LOOKUP_PATH
        target_folder = DE_REG_AUTHORITY_EDGELIST_PATH
        key_df = (
            pd.read_csv(f"{source_folder}/{item}.csv").dropna().set_index("citekey")
        )
        law_citekeys_dict = {
            citekey.split("_")[0]: "_".join(row["key"].split("_")[:-1]) + "_000001"
            for citekey, row in key_df.iterrows()
        }

        df = None
        for file in files:
            edge_df = make_edge_list(file, key_df, law_citekeys_dict, regulations=True)
            df = edge_df if df is None else df.append(edge_df, ignore_index=True)
        df.to_csv(f"{target_folder}/{item}.csv", index=False)


def make_edge_list(file, key_df, law_citekeys_dict, regulations):
    soup = create_soup(
        os.path.join(
            DE_REG_REFERENCE_PARSED_PATH if regulations else DE_REFERENCE_PARSED_PATH,
            file,
        )
    )
    edges = []

    # FOR DEBUG
    problem_matches = set()
    problem_keys = set()

    for item in soup.find_all(["document", "seqitem"], attrs={"parsed": True}):
        item_parsed_ref_str = item.attrs["parsed"]
        if not item_parsed_ref_str or item_parsed_ref_str == "[]":
            continue

        node_out = item.get("key")
        refs = json.loads(item_parsed_ref_str)
        for ref in refs:
            # TODO multiple laws with the same bnormabk
            if len(ref) > 1:  # Ref to seqitem at least
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
                except KeyError:
                    problem_keys.add(key)
            else:  # ref to document only
                node_in = law_citekeys_dict.get(ref[0])
                if node_in:
                    edges.append((node_out, node_in))

    # FOR DEBUG
    # if len(problem_matches) > 0:
    #     print(f"{file} Problem Matches:\n", sorted(list(problem_matches)))
    # if len(problem_keys) > 0:
    #     print(f"{file} Problem Matches:\n", sorted(list(problem_keys)))
    return pd.DataFrame(edges, columns=["out_node", "in_node"])
