import os

import networkx as nx
import pandas as pd
from quantlaw.utils.files import ensure_exists, list_dir

from utils.common import (
    get_snapshot_law_list,
    load_law_names,
)


def crossreference_graph_prepare(
    overwrite, snapshots, source, edgelist_folder, destination
):
    ensure_exists(destination)
    if not snapshots:
        snapshots = sorted(
            set([os.path.splitext(x)[0] for x in list_dir(edgelist_folder, ".csv")])
        )

    if not overwrite:
        existing_files = list_dir(destination, ".gpickle.gz")
        snapshots = list(
            filter(lambda year: f"{year}.gpickle.gz" not in existing_files, snapshots)
        )

    if not len(snapshots):
        return []

    if len(snapshots[0]) == 4:  # is US
        files = []
        for snapshot in snapshots:
            files.append(
                (
                    snapshot,
                    [f"{source}/{x}" for x in os.listdir(source) if str(snapshot) in x],
                )
            )
    else:  # is DE
        files = []
        law_names_data = load_law_names()
        for snapshot in snapshots:
            graph_files = get_snapshot_law_list(snapshot, law_names_data)
            files.append(
                (
                    snapshot,
                    [f'{source}/{x.replace(".xml", ".graphml")}' for x in graph_files],
                )
            )

    return files


def crossreference_graph(args, source, edgelist_folder, destination, add_subseqitems):
    year, files = args

    # make forest from trees
    edge_list = pd.read_csv(f"{edgelist_folder}/{year}.csv")
    G = nx.MultiDiGraph()
    for file in files:
        print(file)
        nG = nx.read_graphml(file)

        # enable filtering by law name
        nx.set_node_attributes(nG, nG.graph.get("name", file), name="law_name")
        G.add_nodes_from(nG.nodes(data=True))
        G.add_edges_from(nG.edges())
    G = nx.MultiDiGraph(G)
    # root means "U.S.C." for US and "Bundesgesetze" for DE (umbrella node)
    G.add_node("root", level=-1, key="root", law_name="root")
    for root in [r for r in G.nodes() if G.nodes[r]["level"] == 0]:
        G.add_edge("root", root, edge_type="containment")
    nx.set_edge_attributes(G, "containment", name="edge_type")

    # connect the forest
    edges = [tuple(edge[1].values) for edge in edge_list.iterrows()]
    for node_from, node_to in edges:
        assert G.has_node(node_from)
        assert G.has_node(node_to)

    G.add_edges_from(edges, edge_type="reference")
    G.graph["name"] = f"{year}"
    # print(
    #     f"{year} graph stats"
    #     f"reference edges:   {len([e for e in G.edges.data() if e[2]['edge_type'] == 'reference'])}"
    #     f"containment edges: {len([e for e in G.edges.data() if e[2]['edge_type'] == 'containment'])}"
    #     f"nodes:             {G.number_of_nodes()}"
    # )
    nx.write_gpickle(G, f"{destination}/{year}.gpickle.gz")
