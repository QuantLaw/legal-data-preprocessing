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
        ensure_exists(self.dest)
        if not snapshots:
            snapshots = sorted(
                set([os.path.splitext(x)[0] for x in list_dir(self.lookup, ".csv")])
            )

        if not overwrite:
            existing_files = os.listdir(self.dest)
            snapshots = list(
                filter(lambda f: get_filename(f) not in existing_files, snapshots)
            )

        return snapshots

    @property
    def dest(self):
        return (
            US_REG_CROSSREFERENCE_EDGELIST_PATH
            if self.regulations
            else US_CROSSREFERENCE_EDGELIST_PATH
        )

    @property
    def lookup(self):
        return (
            US_REG_CROSSREFERENCE_LOOKUP_PATH
            if self.regulations
            else US_CROSSREFERENCE_LOOKUP_PATH
        )

    @property
    def src(self):
        return (
            US_REG_REFERENCE_PARSED_PATH
            if self.regulations
            else US_REFERENCE_PARSED_PATH
        )

    def execute_item(self, item):
        yearfiles = [x for x in list_dir(self.src, ".xml") if str(item) in x]
        key_df = pd.read_csv(f"{self.lookup}/{item}.csv").dropna().set_index("citekey")
        edge_list = []
        for i, yearfile_path in enumerate(yearfiles):
            print(f"\r{item} {i:6} / {len(yearfiles)}", end="")
            edge_list_file = self.make_edge_list(yearfile_path, key_df)
            edge_list.extend(edge_list_file)
        if edge_list:
            df = pd.DataFrame(edge_list, columns=["out_node", "in_node"])
            df.to_csv(f"{self.dest}/{item}.csv", index=False)

    def make_edge_list(self, yearfile_path, key_df):
        soup = create_soup(self.src + "/" + yearfile_path)
        edge_list = []

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
                            edge_list.append([node_out, node_in])
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
        return edge_list


###########
# Functions
###########


def get_filename(date):
    return f"{date}.csv"
