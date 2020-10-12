import itertools
import json
import multiprocessing
import re

import networkx as nx
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import list_dir
from quantlaw.utils.networkx import multi_to_weighted

from statics import DE_DECISIONS_NETWORK, DE_DECISIONS_REFERENCE_PARSED_XML


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


def get_graph_data_from_decision(decision):
    try:
        soup = create_soup(f"{DE_DECISIONS_REFERENCE_PARSED_XML}/{decision}")
        items = list(soup.find_all(["document", "item", "seqitem"]))
        node_dicts = []
        containment_edges = []

        for item in items:
            node_dict = dict(
                key=item.attrs["key"],
                heading=item.attrs.get("heading", ""),
                level=int(item.attrs["level"]),
                type=item.name,
            )

            text = item.get_text(" ")
            node_dict["chars_n"] = count_characters(text, whites=True)
            node_dict["chars_nowhites"] = count_characters(text, whites=False)
            node_dict["tokens_n"] = count_tokens(text, unique=False)
            node_dict["tokens_unique"] = count_tokens(text, unique=True)

            if item.name == "document":
                for key in ["az", "gericht", "datum", "doktyp", "spruchkoerper"]:
                    node_dict[key] = item.attrs.get(key, "")
                parent_key = "root"
            else:
                node_dict["parent_key"] = item.parent.attrs["key"]
                parent_key = item.parent.attrs["key"]

            node_dicts.append(node_dict)
            containment_edges.append((parent_key, item.attrs["key"]))

        reference_edges = []
        for item in items:
            for node in item.find_all("reference"):
                if (
                    node.lawname
                    and "parsed" in node.attrs
                    and node.lawname.get("type")
                    in [
                        "dict",
                        "sgb",
                    ]
                ):
                    refs = json.loads(node.attrs["parsed"])
                    for ref in refs:
                        ref_key = "_".join(ref[:2])
                        reference_edges.append((item.attrs["key"], ref_key))
    except Exception:
        print(decision)
        raise

    return node_dicts, containment_edges, reference_edges


def network():
    decisions = list_dir(DE_DECISIONS_REFERENCE_PARSED_XML, ".xml")
    with multiprocessing.Pool() as p:
        results = p.map(get_graph_data_from_decision, decisions)

    node_dicts = list(itertools.chain.from_iterable([x[0] for x in results]))
    containment_edges = list(itertools.chain.from_iterable([x[1] for x in results]))
    reference_edges = list(itertools.chain.from_iterable([x[2] for x in results]))

    hierarchy_G = nx.DiGraph()
    hierarchy_G.add_node("root", level=-1, key="root", bipartite="decision")
    hierarchy_G.add_nodes_from(
        [(x["key"], x) for x in node_dicts], bipartite="decision"
    )
    hierarchy_G.add_edges_from(containment_edges, edge_type="containment")

    reference_G = nx.MultiDiGraph(hierarchy_G)
    print("created")
    reference_G.add_nodes_from(
        sorted({x[-1] for x in reference_edges}), bipartite="statute"
    )
    print("Statute nodes added")
    reference_G.add_edges_from(reference_edges, edge_type="reference")
    print("Reference edges added")

    reference_weighted_G = multi_to_weighted(reference_G)

    nx.write_gpickle(reference_weighted_G, DE_DECISIONS_NETWORK)
