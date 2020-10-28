import os
import pickle

import networkx as nx
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.networkx import get_leaves
from quantlaw.utils.pipeline import PipelineStep
from regex import regex

from utils.common import get_snapshot_law_list


class SnapshotMappingIndexStep(PipelineStep):
    def __init__(
        self,
        source_graph,
        source_text,
        destination,
        dataset,
        law_names_data=None,
        *args,
        **kwargs,
    ):
        self.source_graph = source_graph
        self.source_text = source_text
        self.destination = destination
        self.dataset = dataset
        self.law_names_data = law_names_data
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(self.destination)
        files = sorted(list_dir(self.source_graph, ".gpickle.gz"))
        items = [f[: -len(".gpickle.gz")] for f in files]

        if snapshots:
            items = list(filter(lambda i: i in snapshots, items))

        if not overwrite:
            existing_files = list_dir(self.destination, ".pickle")
            items = list(filter(lambda x: (x + ".pickle") not in existing_files, items))
        return items

    def execute_item(self, item):
        # Load structure
        G = load_crossref_graph(item, self.source_graph)

        # Load texts
        leave_texts = list(
            get_leaf_texts_to_compare(
                item, G, self.source_text, self.law_names_data, self.dataset
            )
        )

        self.save_raw(item, G, leave_texts)
        del G

        # self.save_index(item, leave_texts)

    def save_raw(self, item, G, leave_texts):

        keys = [key for key, text in leave_texts]
        texts = [text for key, text in leave_texts]

        citekeys_dict = nx.get_node_attributes(G, "citekey")
        citekeys = [citekeys_dict.get(key) for key, text in leave_texts]

        pickle_path = os.path.join(self.destination, item + ".pickle")

        with open(pickle_path, "wb") as f:
            pickle.dump(dict(keys=keys, texts=texts, citekeys=citekeys), f)

    # def save_index(self, item, leave_texts):
    #     index_dir = os.path.join(self.destination, item + "_index")
    #
    #     if os.path.exists(index_dir):
    #         shutil.rmtree(index_dir)
    #     os.makedirs(index_dir)
    #
    #     snapshot_mapping_schema = Schema(
    #         content=TEXT(analyzer=KeywordAnalyzer(lowercase=True, commas=False)),
    #         content_len=NUMERIC,
    #         key=ID(stored=True),
    #     )
    #
    #     ix = create_in(index_dir, snapshot_mapping_schema)
    #     writer = ix.writer()
    #     for key, text in leave_texts:
    #         writer.add_document(content=text, key=key, content_len=len(text))
    #
    #     writer.commit()


def load_crossref_graph(item, source):
    graph_path = f"{source}/{item}.gpickle.gz"
    G = nx.read_gpickle(graph_path)
    return G


def get_leaf_texts_to_compare(snapshot, G, source_text, law_names_data, dataset):
    """
    get text for leaves of a hierarchy graph. Can be seqitem or supseqitem graph.
    Leaves are only seqitems or supseqitems.
    """
    leaf_keys = get_leaves(G)

    if dataset == "us":
        files = sorted(
            [
                x
                for x in list_dir(source_text, ".xml")
                if x.split(".")[0].split("_")[-1] == snapshot
            ]
        )
    else:  # is DE
        files = get_snapshot_law_list(snapshot, law_names_data)

    whitespace_pattern = regex.compile(r"[\s\n]+")
    for file in files:
        soup = create_soup(f"{source_text}/{file}")
        tags = soup.find_all(["seqitem", "subseqitem"])
        for tag in tags:
            if tag["key"] in leaf_keys:
                text = tag.get_text(" ")
                text = whitespace_pattern.sub(" ", text).lower().strip()
                yield tag["key"], text.lower()
