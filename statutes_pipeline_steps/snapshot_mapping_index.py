import os
import pickle

import networkx as nx
import pandas as pd
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists, list_dir
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
        files = sorted(list_dir(self.source_graph, ".nodes.csv.gz"))
        items = [f[: -len(".nodes.csv.gz")] for f in files]

        if snapshots:
            items = list(filter(lambda i: i in snapshots, items))

        if not overwrite:
            existing_files = list_dir(self.destination, ".pickle")
            items = list(filter(lambda x: (x + ".pickle") not in existing_files, items))
        return items

    def get_leaves_with_citekeys(self, item):
        nodes_csv_path = os.path.join(self.source_graph, f"{item}.nodes.csv.gz")
        edges_csv_path = os.path.join(self.source_graph, f"{item}.edges.csv.gz")

        connecting_nodes = set()
        for edges_df in pd.read_csv(edges_csv_path, chunksize=10000):
            connecting_nodes.update(
                edges_df[edges_df.edge_type == "containment"].u.to_list()
            )

        leaves = []
        for nodes_df in pd.read_csv(nodes_csv_path, chunksize=10000):
            for idx, row in nodes_df.iterrows():
                if row.key not in connecting_nodes:  # it's a leaf
                    leaves.append(
                        (row.key, None if pd.isnull(row.citekey) else row.citekey)
                    )

        return leaves

    def execute_item(self, item):
        # Load structure
        leaves_with_citekeys = self.get_leaves_with_citekeys(item)

        # Load texts
        leave_texts = list(
            get_leaf_texts_to_compare(
                item,
                leaves_with_citekeys,
                self.source_text,
                self.law_names_data,
                self.dataset,
            )
        )

        self.save_raw(item, leaves_with_citekeys, leave_texts)

    def save_raw(self, item, leaves_with_citekeys, leave_texts):

        keys = [key for key, text in leave_texts]
        texts = [text for key, text in leave_texts]

        citekeys_dict = {key: citekey for key, citekey in leaves_with_citekeys}
        citekeys = [citekeys_dict[key] for key, text in leave_texts]

        pickle_path = os.path.join(self.destination, item + ".pickle")

        with open(pickle_path, "wb") as f:
            pickle.dump(dict(keys=keys, texts=texts, citekeys=citekeys), f)


def load_crossref_graph(item, source):
    graph_path = f"{source}/{item}.gpickle.gz"
    G = nx.read_gpickle(graph_path)
    return G


def get_leaf_texts_to_compare(
    snapshot, leaves_with_citekeys, source_texts, law_names_data, dataset
):
    """
    get text for leaves of a hierarchy graph. Can be seqitem or supseqitem graph.
    Leaves are only seqitems or supseqitems.
    """

    leaf_keys = {key for key, citekey in leaves_with_citekeys}

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
        soup = create_soup(file)
        tags = soup.find_all(["seqitem", "subseqitem"])
        for tag in tags:
            if tag["key"] in leaf_keys:
                text = tag.get_text(" ")
                text = whitespace_pattern.sub(" ", text).lower().strip()
                yield tag["key"], text.lower()
