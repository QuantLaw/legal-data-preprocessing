import os

import networkx as nx
import pandas as pd

from legal_data_common_utils.common import ensure_exists, list_dir, get_snapshot_law_list, load_law_names


def get_filename(year):
    return f"{year}.graphml"


def crossreference_graph_prepare(
    overwrite, snapshots, source, edgelist_folder, destination
):
    ensure_exists(destination)
    if not snapshots:
        snapshots = sorted(
            set([os.path.splitext(x)[0] for x in list_dir(edgelist_folder, ".csv")])
        )
    # TODO LATER also check source (hierarchical graphml files)

    if not overwrite:
        existing_files = list_dir(destination, ".graphml")
        snapshots = list(
            filter(lambda date: get_filename(date) not in existing_files, snapshots)
        )

    if not len(snapshots):
        return []

    if len(snapshots[0]) == 4:  # TODO LATER is us
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
            graphml_files = get_snapshot_law_list(snapshot, law_names_data)
            files.append(
                (
                    snapshot,
                    [
                        f'{source}/{x.replace(".xml", ".graphml")}'
                        for x in graphml_files
                    ],
                )
            )

    return files


def crossreference_graph(args, source, edgelist_folder, destination, add_subseqitems):
    year, files = args
    # TODO LATER implement add_subseqitems. If referenced to subseqitem but not in list, reduce key until match

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
    # TODO LATER remove graphml and use gpickle.gz only (faster and more space efficient)
    nx.write_graphml(G, f"{destination}/{year}.graphml")
    nx.write_gpickle(
        G, f"{destination}/{year}.gpickle.gz"
    )  # Add for faster loading e.g. in community detection
