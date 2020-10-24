import itertools
import json

import lxml.etree

from statics import US_REG_AUTHORITY_EDGELIST_PATH
from statutes_pipeline_steps.us_crossreference_edgelist import UsCrossreferenceEdgelist


class UsAuthorityEdgelist(UsCrossreferenceEdgelist):
    @property
    def dest(self):
        assert self.regulations
        return US_REG_AUTHORITY_EDGELIST_PATH

    def make_edge_list(self, yearfile_path, key_dict):
        with open(yearfile_path, encoding="utf8") as f:
            file_elem = lxml.etree.parse(f)
        edge_list = []

        # for debug
        # problem_matches = set()
        # problem_keys = set()

        for item in file_elem.xpath("//*[@auth_text_parsed]"):
            node_out = item.attrib.get("key")
            refs = itertools.chain.from_iterable(
                json.loads(item.attrib["auth_text_parsed"])
            )
            for ref in refs:
                key = "_".join(ref[:2])
                node_in = key_dict.get(key)

                if node_in:
                    edge_list.append([node_out, node_in])

        return edge_list
