import json
import os

import lxml.etree
import pandas as pd
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
    def __init__(self, detailed_crossreferences, *args, **kwargs):
        self.detailed_crossreferences = detailed_crossreferences
        super().__init__(*args, **kwargs)

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
        ) + ("/detailed" if self.detailed_crossreferences else "")

    @property
    def lookup(self):
        return (
            US_REG_CROSSREFERENCE_LOOKUP_PATH
            if self.regulations
            else US_CROSSREFERENCE_LOOKUP_PATH
        ) + ("/detailed" if self.detailed_crossreferences else "")

    def execute_item(self, item):
        yearfiles = [
            os.path.join(US_REFERENCE_PARSED_PATH, x)
            for x in list_dir(US_REFERENCE_PARSED_PATH, ".xml")
            if str(item) in x
        ]
        if self.regulations:
            yearfiles += [
                os.path.join(US_REG_REFERENCE_PARSED_PATH, x)
                for x in list_dir(US_REG_REFERENCE_PARSED_PATH, ".xml")
                if str(item) in x
            ]

        key_df = pd.read_csv(f"{self.lookup}/{item}.csv").dropna().set_index("citekey")
        key_dict = {}
        for idx, val in key_df.key.iteritems():
            if idx not in key_dict:
                key_dict[idx] = val
        edge_list = []
        for yearfile_path in yearfiles:
            edge_list_file = self.make_edge_list(yearfile_path, key_dict)
            edge_list.extend(edge_list_file)
        if edge_list:
            df = pd.DataFrame(edge_list, columns=["out_node", "in_node"])
            df.to_csv(f"{self.dest}/{item}.csv", index=False)

    def make_edge_list(self, yearfile_path, key_dict):
        with open(yearfile_path, encoding="utf8") as f:
            file_elem = lxml.etree.parse(f)
        edge_list = []

        if self.detailed_crossreferences:
            for ref_elem in file_elem.xpath(".//reference"):
                node_out = ref_elem.getparent().getparent().attrib.get("key")
                refs = json.loads(ref_elem.attrib["parsed"])
                for ref in refs:
                    for cutoff in range(len(ref), 1, -1):
                        key = "_".join(ref[:cutoff])
                        node_in = key_dict.get(key)
                        if node_in:
                            edge_list.append([node_out, node_in])
                            break
        else:
            for seqitem_elem in file_elem.xpath("//seqitem"):
                node_out = seqitem_elem.attrib.get("key")
                for ref_elem in seqitem_elem.xpath(".//reference"):
                    refs = json.loads(ref_elem.attrib["parsed"])
                    for ref in refs:
                        for cutoff in range(len(ref), 1, -1):
                            key = "_".join(ref[:cutoff])
                            node_in = key_dict.get(key)
                            if node_in:
                                edge_list.append([node_out, node_in])
                                break
        return edge_list


###########
# Functions
###########


def get_filename(date):
    return f"{date}.csv"
