import json
import os

import pandas as pd
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists, list_dir

from statics import (
    US_CROSSREFERENCE_EDGELIST_PATH,
    US_CROSSREFERENCE_LOOKUP_PATH,
    US_REFERENCE_PARSED_PATH,
    US_REG_CROSSREFERENCE_EDGELIST_PATH,
    US_REG_CROSSREFERENCE_LOOKUP_PATH,
    US_REG_REFERENCE_PARSED_PATH,
)
from utils.common import RegulationsPipelineStep


class UsCrossreferenceEdgelist(RegulationsPipelineStep):
    def get_items(self, overwrite, snapshots) -> list:
        dest = (
            US_REG_CROSSREFERENCE_EDGELIST_PATH
            if self.regulations
            else US_CROSSREFERENCE_EDGELIST_PATH
        )
        lookup = (
            US_REG_CROSSREFERENCE_LOOKUP_PATH
            if self.regulations
            else US_CROSSREFERENCE_LOOKUP_PATH
        )

        ensure_exists(dest)
        if not snapshots:
            snapshots = sorted(
                set([os.path.splitext(x)[0] for x in list_dir(lookup, ".csv")])
            )

        if not overwrite:
            existing_files = os.listdir(dest)
            snapshots = list(
                filter(lambda f: get_filename(f) not in existing_files, snapshots)
            )

        return snapshots

    def execute_item(self, item):
        dest = (
            US_REG_CROSSREFERENCE_EDGELIST_PATH
            if self.regulations
            else US_CROSSREFERENCE_EDGELIST_PATH
        )
        lookup = (
            US_REG_CROSSREFERENCE_LOOKUP_PATH
            if self.regulations
            else US_CROSSREFERENCE_LOOKUP_PATH
        )
        src = (
            US_REG_REFERENCE_PARSED_PATH
            if self.regulations
            else US_REFERENCE_PARSED_PATH
        )

        yearfiles = [x for x in list_dir(src, ".xml") if str(item) in x]
        key_df = pd.read_csv(f"{lookup}/{item}.csv").dropna().set_index("citekey")
        df = None
        for yearfile_path in yearfiles:
            edge_df = self.make_edge_list(yearfile_path, key_df)
            df = edge_df if df is None else df.append(edge_df, ignore_index=True)
        if df:
            df.to_csv(f"{dest}/{item}.csv", index=False)

    def make_edge_list(self, yearfile_path, key_df):
        soup = create_soup(
            (
                US_REG_REFERENCE_PARSED_PATH
                if self.regulations
                else US_REFERENCE_PARSED_PATH
            )
            + "/"
            + yearfile_path
        )
        edge_df = pd.DataFrame(columns=["out_node", "in_node"])

        # for debug
        # problem_matches = set()
        # problem_keys = set()

        for item in soup.find_all(["seqitem"]):
            if item.find_all(["reference"]):
                node_out = item.get("key")
                for node in item.find_all(["reference"]):
                    refs = json.loads(node.attrs["parsed"])
                    for ref in refs:
                        try:  # for debug
                            key = "_".join(ref[:2])
                            matches = key_df.at[key, "key"]

                            # # for debug
                            # if type(matches) != str:
                            #     problem_matches.add(tuple(matches))

                            node_in = matches if type(matches) == str else matches[0]
                            edge_df = edge_df.append(
                                pd.DataFrame(
                                    dict(in_node=[node_in], out_node=[node_out])
                                ),
                                ignore_index=True,
                            )
                            assert len(ref) > 1

                        except KeyError:
                            # # for debug
                            # problem_keys.add(key)
                            pass

        # # for debug
        # if len(problem_matches) > 0:
        #     print(f"{yearfile_path} Problem Matches:\n",
        #           sorted(list(problem_matches)))
        # if len(problem_keys) > 0:
        #     print(f"{yearfile_path} Problem Matches:\n", sorted(list(problem_keys)))
        return edge_df[["out_node", "in_node"]]


###########
# Functions
###########


def get_filename(date):
    return f"{date}.csv"
