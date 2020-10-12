import os
import re

import networkx as nx
from bs4 import BeautifulSoup

from utils.common import ensure_exists, list_dir


def get_graphml_filename(filename):
    return f"{os.path.splitext(filename)[0]}.graphml"


def hierarchy_graph_prepare(overwrite, source, destination):
    ensure_exists(destination)
    files = list_dir(source, ".xml")

    if not overwrite:
        existing_files = list_dir(destination, ".graphml")
        files = list(
            filter(lambda f: get_graphml_filename(f) not in existing_files, files)
        )

    return files


def hierarchy_graph(filename, source, destination, add_subseqitems):
    G = build_graph(f"{source}/{filename}", add_subseqitems=add_subseqitems)

    destination_path = f"{destination}/{get_graphml_filename(filename)}"
    nx.write_graphml(G, destination_path)


###########
# Functions
###########


def add_juris_attrs(item, node_attrs):
    if item.attrs.get("normgeber"):
        node_attrs["legislators"] = item.attrs["normgeber"]
    if item.attrs.get("mitwirkende"):
        node_attrs["contributors"] = item.attrs["mitwirkende"]
    if item.attrs.get("sachgebiete"):
        node_attrs["subject_areas"] = item.attrs["sachgebiete"]


def nest_items(G, items, document_type):
    for item in items:
        if type(item.parent) is not BeautifulSoup:
            node_attrs = dict(
                key=item.attrs["key"],
                citekey=item.attrs.get("citekey", ""),
                heading=item.attrs.get("heading", ""),
                parent_key=item.parent.attrs["key"],
                level=int(item.attrs["level"]),
                type=item.name,
            )
            if document_type:
                node_attrs["document_type"] = document_type
            add_juris_attrs(item, node_attrs)

            G.add_node(item.attrs["key"], **node_attrs)
            G.add_edge(item.parent.attrs["key"], item.attrs["key"])

        else:  # handle root node
            node_attrs = dict(
                key=item.attrs["key"],
                citekey=item.attrs.get("citekey", ""),
                heading=item.attrs.get("heading", ""),
                parent_key="",
                level=int(item.attrs["level"]),
                type=item.name,
                **(dict(document_type=document_type) if document_type else {}),
            )
            if "abbr_1" in item.attrs:
                node_attrs["abbr_1"] = item.attrs["abbr_1"]
            if "abbr_2" in item.attrs:
                node_attrs["abbr_2"] = item.attrs["abbr_2"]
            add_juris_attrs(item, node_attrs)

            G.add_node(item.attrs["key"], **node_attrs)
            G.graph["name"] = item.attrs.get("heading", "")

    return G


def count_characters(text, whites=False):
    if whites:
        return len(text)
    else:
        return len(re.sub(r"\s", "", text))


def count_tokens(text, unique=False):
    if not unique:
        return len(text.split())
    else:
        return len(set(text.split()))


def build_graph(filename, add_subseqitems=False):
    """Builds an awesome graph from a file."""
    with open(filename, encoding="utf8") as f:
        soup = BeautifulSoup(f.read(), "lxml-xml")

    document_type = soup.document.attrs.get("document_type", None)

    G = nx.DiGraph()

    items = soup.find_all(["document", "item", "seqitem"])
    G = nest_items(G, items, document_type)
    subitems = []
    if add_subseqitems:
        subitems = soup.find_all("subseqitem")
        G = nest_items(G, subitems, document_type)

    for item in items + subitems:
        text = item.get_text(" ")
        G.nodes[item.attrs["key"]]["chars_n"] = count_characters(text, whites=True)
        G.nodes[item.attrs["key"]]["chars_nowhites"] = count_characters(
            text, whites=False
        )
        G.nodes[item.attrs["key"]]["tokens_n"] = count_tokens(text, unique=False)
        G.nodes[item.attrs["key"]]["tokens_unique"] = count_tokens(text, unique=True)

    return G
