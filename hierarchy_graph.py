import os
import re

import networkx as nx
from bs4 import BeautifulSoup

from common import ensure_exists, list_dir


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


def nest_items(G, items):
    for item in items:
        if type(item.parent) is not BeautifulSoup:
            G.add_node(
                item.attrs["key"],
                key=item.attrs["key"],
                citekey=item.attrs.get("citekey", ""),
                heading=item.attrs.get("heading", ""),
                parent_key=item.parent.attrs["key"],
                level=int(item.attrs["level"]),
                type=item.name,
            )
            G.add_edge(item.parent.attrs["key"], item.attrs["key"])
        # handle root node
        else:
            G.add_node(
                item.attrs["key"],
                key=item.attrs["key"],
                citekey=item.attrs.get("citekey", ""),
                heading=item.attrs.get("heading", ""),
                parent_key="",
                level=int(item.attrs["level"]),
                type=item.name,
            )
            G.graph["name"] = item.attrs.get("heading", "")
    validate_graph(G)
    return G


def validate_graph(G):
    pass
    # TODO LATER integrate test but handle repealed/empty laws correctly
    # assert G.number_of_edges() == G.number_of_nodes() - 1, f"Failed on n-1: {G.graph['name']}, {list(G.nodes)[0]}" # necessary for being a tree
    # assert max([d for _, d in G.in_degree()]) == 1, f"Failed on indegree test: {G.graph['name']}, {list(G.nodes)[0]}" # combined with the above, sufficient for being a tree


def count_characters(text, whites=False):
    if whites:
        return len(text)
    else:
        return len(re.sub("\s", "", text))


def count_tokens(text, unique=False):
    if not unique:
        return len(text.split())
    else:
        return len(set(text.split()))


def build_graph(filename, add_subseqitems=False):
    """Builds an awesome graph from a file."""
    with open(filename, encoding="utf8") as f:
        soup = BeautifulSoup(f.read(), "lxml-xml")

    G = nx.DiGraph()

    items = soup.find_all(["document", "item", "seqitem"])
    G = nest_items(G, items)
    subitems = []
    if add_subseqitems:
        subitems = soup.find_all("subseqitem")
        G = nest_items(G, subitems)

    for item in items + subitems:
        text = item.get_text(" ")
        G.nodes[item.attrs["key"]]["chars_n"] = count_characters(text, whites=True)
        G.nodes[item.attrs["key"]]["chars_nowhites"] = count_characters(
            text, whites=False
        )
        G.nodes[item.attrs["key"]]["tokens_n"] = count_tokens(text, unique=False)
        G.nodes[item.attrs["key"]]["tokens_unique"] = count_tokens(text, unique=True)

    return G
