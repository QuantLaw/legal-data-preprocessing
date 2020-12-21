import os
import pickle

import networkx as nx
from lxml import etree
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.pipeline import PipelineStep
from regex import regex

from utils.common import get_snapshot_law_list


class SnapshotMappingIndexStep(PipelineStep):
    def __init__(
        self,
        source_text,
        destination,
        dataset,
        law_names_data=None,
        *args,
        **kwargs,
    ):
        self.source_text = source_text
        self.destination = destination
        self.dataset = dataset
        self.law_names_data = law_names_data
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(self.destination)
        items = snapshots
        if not overwrite:
            existing_files = list_dir(self.destination, ".pickle")
            items = list(filter(lambda x: (x + ".pickle") not in existing_files, items))
        return items

    def execute_item(self, item):
        # Load texts
        item_data = list(
            get_texttags_to_compare(
                item,
                self.source_text,
                self.law_names_data,
                self.dataset,
            )
        )

        self.save_raw(item, item_data)

    def save_raw(self, item, item_data):

        keys, citekeys, texts = list(zip(*item_data))

        pickle_path = os.path.join(self.destination, item + ".pickle")

        with open(pickle_path, "wb") as f:
            pickle.dump(dict(keys=keys, texts=texts, citekeys=citekeys), f)


def load_crossref_graph(item, source):
    graph_path = f"{source}/{item}.gpickle.gz"
    G = nx.read_gpickle(graph_path)
    return G


def get_texttags_to_compare(snapshot, source_texts, law_names_data, dataset):

    if dataset == "us":
        if type(source_texts) is str:
            source_texts = [source_texts]

        files = sorted(
            [
                os.path.join(source_text, x)
                for source_text in source_texts
                for x in list_dir(source_text, ".xml")
                if x.split(".")[0].split("_")[-1] == snapshot
            ]
        )
    else:  # is DE
        assert type(source_texts) is str
        files = get_snapshot_law_list(snapshot, law_names_data)
        files = [os.path.join(source_texts, f) for f in files]

    whitespace_pattern = regex.compile(r"[\s\n]+")

    for file in files:
        tree = etree.parse(file)
        for text_tag in tree.xpath("//text"):
            item = text_tag.getparent()

            pos_in_item = item.getchildren().index(text_tag)
            text_key = item.attrib["key"] + f"_{pos_in_item}"

            seqitem = get_seqitem(item)
            if seqitem is not None:
                citekey = seqitem.attrib.get("citekey")
            else:
                citekey = None

            text = etree.tostring(text_tag, method="text", encoding="utf8").decode(
                "utf-8"
            )
            text = whitespace_pattern.sub(" ", text).lower().strip()

            yield text_key, citekey, text


def get_seqitem(elem):
    if elem is None:
        return None
    elif elem.tag == "seqitem":
        return elem
    return get_seqitem(elem.getparent())
