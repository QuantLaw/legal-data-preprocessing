import itertools
import os
from collections import Counter

import matplotlib.pyplot as plt
import networkx as nx
import seaborn as sns
from cdlib import NodeClustering, readwrite

from common import create_soup, get_snapshot_law_list
from statics import US_REFERENCE_PARSED_PATH, DE_REFERENCE_PARSED_PATH

sns.set_style("white")


###################
# General Helpers
###################


def multi_to_weighted(G):
    """
    Converts a multidigraph into a weighted digraph.
    """
    nG = nx.DiGraph(G)
    # nG.add_nodes_from(G.nodes)
    nG.name = G.name + "_weighted_nomulti"
    edge_weights = {(u, v): 0 for u, v, k in G.edges}
    for u, v, key in G.edges:
        edge_weights[(u, v)] += 1
    # nG.add_edges_from(edge_weights.keys())
    nx.set_edge_attributes(nG, edge_weights, "weight")
    return nG


def filter_edges(G, edge_attr, edge_val_to_remove):
    """
    Create a new graph with all nodes in G and edges of type 'edge_type_to_remove' removed.
    """
    nG = type(G)()  # construct graph of same type as G
    nG.add_nodes_from(G.nodes(data=True))
    nG.add_edges_from(
        [x for x in G.edges(data=True) if x[-1].get(edge_attr) != edge_val_to_remove]
    )
    return nG


###################
# Basic Graph Types
###################


def induced_subgraph(G, filter_type, filter_attribute, filter_values):
    """
    Create custom induced subgraph.
    :param filter_type: node|edge
    :param filter_attribute: attribute to filter on
    :param filter_values: attribute values to evaluate to True
    """
    G = nx.MultiDiGraph(G)
    if filter_type == "node":
        nodes = [n for n in G.nodes() if G.nodes[n].get(filter_attribute) in filter_values]
        sG = nx.induced_subgraph(G, nodes)
    elif filter_type == "edge":
        sG = nx.MultiDiGraph()
        sG.add_nodes_from(G.nodes(data=True))
        sG.add_edges_from(
            [
                (e[0], e[1], e[-1])
                for e in G.edges(data=True)
                if e[-1][filter_attribute] in filter_values
            ]
        )
    else:
        raise
    sG.graph["name"] = "_".join(
        [G.graph["name"], filter_type, filter_attribute, str(*filter_values)]
    )
    return sG


def hierarchy_graph(G):
    """
    Remove reference edges from G.
    Wrapper around induced_subgraph.
    """
    hG = induced_subgraph(G, "edge", "edge_type", ["containment"])
    return hG


def quotient_graph(
    G, node_attribute, edge_types=["reference"], self_loops=False, root_level=-1
):
    """
    Generate the quotient graph with all nodes sharing the same node_attribute condensed into a single node.
    Attribute aggregation functions not currently implemented; primary use case currently aggregation by law_name.
    """

    # node_key:attribute_value map
    attribute_data = dict(G.nodes(data=node_attribute))
    # set cluster -1 if they were not part of the clustering (guess: those are empty laws)
    attribute_data = {
        k: (v if v is not None else -1) for k, v in attribute_data.items()
    }
    # unique values in that map
    unique_values = sorted(list(set(attribute_data.values())))

    # remove the root if root_level is given
    if root_level is not None:
        root = [x for x in G.nodes() if G.nodes[x]["level"] == root_level][0]
        unique_values.remove(root)
    else:
        root = None

    # build a new MultiDiGraph
    nG = nx.MultiDiGraph()

    # add nodes
    new_nodes = {x: [] for x in unique_values}
    nG.add_nodes_from(unique_values)

    # sort nodes into buckets
    for n in attribute_data.keys():
        if n != root:
            mapped_to = attribute_data[n]
            new_nodes[mapped_to].append(n)
            if G.nodes[n].get("heading") == mapped_to:
                for x in G.nodes[n].keys():
                    nG.nodes[mapped_to][x] = G.nodes[n][x]

    # add edges
    for e in G.edges(data=True):
        if e[-1]["edge_type"] not in edge_types:
            continue
        if (True if self_loops else attribute_data[e[0]] != attribute_data[e[1]]) and (
            True if root_level is None else G.nodes[e[0]]["level"] != root_level
        ):  # special treatment for root
            k = nG.add_edge(
                attribute_data[e[0]], attribute_data[e[1]], edge_type=e[-1]["edge_type"]
            )
            if e[-1]["edge_type"] == "sequence":
                nG.edges[attribute_data[e[0]], attribute_data[e[1]], k]["weight"] = e[
                    -1
                ]["weight"]

    nG.graph["name"] = f'{G.graph["name"]}_quotient_graph_{node_attribute}'

    return nG


def decay_function(key):
    """
    Returns a decay function to create a weighted sequence graph.
    """
    return lambda x: (x - 1) ** (-key)


def sequence_graph(G, seq_decay_func=decay_function(1), seq_ref_ratio=1):
    """
    Creates sequence graph for G, consisting of seqitems and their cross-references only,
    where neighboring seqitems are connected via edges in both directions.
    :param seq_decay_func: function to calculate sequence edge weight based on distance between neighboring nodes
    :param seq_ref_ratio: ratio between a sequence edge weight when nodes in the sequence are at minimum distance
           from each other and a reference edge weight
    """

    # TODO LATER perhaps create more flexible interface
    #  (allow to specify whether the seqgraph should be undirected and simple, e.g.)
    hG = hierarchy_graph(G)
    # make sure we get _all_ seqitems as leaves, not only the ones without outgoing references
    leaves = [n for n in hG.nodes() if hG.out_degree(n) == 0]

    sG = nx.MultiDiGraph(nx.induced_subgraph(G, leaves))

    if seq_ref_ratio:
        nx.set_edge_attributes(sG, 1 / seq_ref_ratio, name="weight")
        node_headings = dict(sG.nodes(data="heading"))
        ordered_seqitems = sorted(list(node_headings.keys()))

        # connect neighboring seqitems sequentially
        new_edges = get_new_edges(G, ordered_seqitems, seq_decay_func)
        sG.add_edges_from(new_edges)
    else:
        nx.set_edge_attributes(sG, 1, name="weight")


    sG.graph["name"] = f'{G.graph["name"]}_sequence_graph_seq_ref_ratio_{seq_ref_ratio}'

    return sG


def get_new_edges(G, ordered_seqitems, seq_decay_func):
    """
    Convenience function to avoid list comprehension over four lines.
    """
    there = []
    back = []
    hG = hierarchy_graph(G).to_undirected()
    for idx, n in enumerate(ordered_seqitems[:-1]):
        next_item = ordered_seqitems[idx + 1]
        if (
            n.split("_")[0] == next_item.split("_")[0]
        ):  # n and next_item are in the same law
            distance = nx.shortest_path_length(hG, source=n, target=next_item)
            weight = seq_decay_func(distance)
            there.append(
                (
                    n,
                    next_item,
                    {"edge_type": "sequence", "weight": weight, "backwards": False},
                )
            )
            back.append(
                (
                    next_item,
                    n,
                    {"edge_type": "sequence", "weight": weight, "backwards": True},
                )
            )
    return there + back


#######################
# Get clustering result
#######################


def add_communities_to_graph(clustering: NodeClustering):
    """
    Assign community labels to nodes of the graph, propagating community labels from higher levels down the tree.
    """
    community_attrs = {}
    cluster_object_attrs = {}
    hG = hierarchy_graph(clustering.graph)
    for node_key, community_ids in clustering.to_node_community_map().items():
        node_with_descendants = [node_key] + [n for n in nx.descendants(hG, node_key)]
        for node in node_with_descendants:
            community_attrs[node] = community_ids
        cluster_object_attrs[node] = True

    nx.set_node_attributes(clustering.graph, community_attrs, "communities")
    nx.set_node_attributes(clustering.graph, cluster_object_attrs, "clusterobject")


def add_community_to_graph(clustering: NodeClustering):
    communities = nx.get_node_attributes(clustering.graph, "communities")
    if not len(communities):
        raise Exception(
            "No communities found in graph. "
            'Call "add_communities_to_graph" before calling "add_community_to_graph"'
        )
    for node, community_attr in communities.items():
        if len(community_attr) > 1:
            raise Exception(
                f"Node {node} has too many communities assigned: {community_attr}"
            )

    community = {k: v[0] for k, v in communities.items()}
    nx.set_node_attributes(clustering.graph, community, "community")


def get_clustering_result(cluster_path, dataset, graph_type):
    """
    read the clustering result and the respective graph.
    ::param cluster_path: path of the cdlib.readwrite.write_community_json output
    ::param dataset: 'de' or 'us'
    ::param graph_type: 'clustering' for the rolled up graph. Other options: subseqitems, seqitems
    """

    # TODO LATER use statics for paths

    filename_base = os.path.splitext(os.path.split(cluster_path)[-1])[0]
    snapshot = filename_base.split("_")[0]
    dataset_folder = f"{dataset.upper()}-data"

    if graph_type == "clustering":
        graph_filename = "_".join(filename_base.split("_")[:4])
        graph_path = (
            f"../{dataset_folder}/cd_1_preprocessed_graph/{graph_filename}.gpickle.gz"
        )
        G = nx.read_gpickle(graph_path)
    elif graph_type in ["seqitems", "subseqitems"]:
        if dataset == "de":
            graph_folder = "11_crossreference_graph"
        elif dataset == "us":
            graph_folder = "7_crossreference_graph"
        else:
            raise Exception(f"dataset {dataset} is not an allowed")
        graph_path = (
            f"../{dataset_folder}/{graph_folder}/{graph_type}/{snapshot}.gpickle.gz"
        )
        G = nx.read_gpickle(graph_path)

    else:
        raise Exception(f"graph_type {graph_type} not allowed")

    clustering = readwrite.read_community_json(cluster_path)
    clustering.graph = G

    add_communities_to_graph(clustering)

    return clustering


def get_community_ids(clustering: NodeClustering):
    return sorted(
        set(itertools.chain.from_iterable(clustering.to_node_community_map().values()))
    )


def community_quotient_graph(clustering: NodeClustering):
    add_community_to_graph(clustering)
    sG = sequence_graph(clustering.graph, lambda x: 1 / x, 1)
    qsG = quotient_graph(
        sG, "community", edge_types=["reference", "sequence"], root_level=None
    )
    return qsG


def get_community_law_name_counters(clustering: NodeClustering, count_level: str):
    """
    Counting the law_names in each cluster.
    :param clustering:
    :param count_level:
    The level at which nodes will be counted.
    The clustering must have at least the granularity of the count_level.
    Eg graph_type=clustering and count_level=seqitem is not allowed.
    :return: dict with community ids and  counters
    """

    if count_level == "seqitems":
        node_type = "seqitem"
    elif count_level == "subseqitems":
        node_type = "subseqitem"
    elif count_level == "clustering":
        raise Exception(f"Not yet implemented")
    else:
        raise Exception(f"Wrong argument {count_level}")

    leaves_data_at_level = [
        data
        for n, data in clustering.graph.nodes(data=True)
        # exclude root and filter afterwards
        if data["level"] != -1 and data["type"] == node_type
    ]
    counters = dict()
    for community_id in range(len(clustering.communities)):
        counters[community_id] = Counter(
            [
                "_".join(data["key"].split("_")[:-1])
                for data in leaves_data_at_level
                if data.get("community") == community_id
            ]
        )
    return counters


####################################
# Rollup Helpers and Rolled-Up Graph
####################################


def get_leaves(G):
    H = hierarchy_graph(G)
    return set([node for node in H.nodes if H.out_degree(node) == 0])


def get_leaves_with_communities(G):
    return {
        node: G.nodes[node]["communities"]
        for node in get_leaves(G)
        if "communities"
        in G.nodes[node]  # TODO LATER subseqitems do not have a parent seqitem
    }


def get_item_content(country, itemkeys, snapshot, include_supseqitems=False):
    """
    Creates a dictionary with the †ext content of the gives itemskeys.
    :param country: de or us
    :param subseqitem: Set to true, if itemskeys contain keys of subseqitems.
           Default: false. Reason: Performance
    :param snapshot:
    :return:
    """
    texts = {}
    if country.lower() == "us":
        for file in sorted(
            [
                f
                for f in os.listdir(US_REFERENCE_PARSED_PATH)
                if f.endswith(f"_{snapshot}.xml")
            ]
        ):
            soup = create_soup(f"{US_REFERENCE_PARSED_PATH}/{file}")
            tags = soup.find_all(
                ["subseqitem", "seqitem", "item", "document"]
                if include_supseqitems
                else ["seqitem", "item", "document"]
            )
            for tag in tags:
                if tag["key"] in itemkeys:
                    texts[tag["key"]] = tag.get_text(" ")

    else:  # de
        files = get_snapshot_law_list(snapshot)
        texts = {}
        for file in files:
            soup = create_soup(f"{DE_REFERENCE_PARSED_PATH}/{file}")
            tags = soup.find_all(
                ["subseqitem", "seqitem", "item", "document"]
                if include_supseqitems
                else ["seqitem", "item", "document"]
            )
            for tag in tags:
                if tag["key"] in itemkeys:
                    texts[tag["key"]] = tag.get_text(" ")

    return texts


def get_heading_path(G, node):
    if "heading" not in G.nodes[node]:
        return tuple()

    heading = G.nodes[node]["heading"]
    parents = list(G.predecessors(node))
    if parents:
        return (*get_heading_path(G, parents[0]), heading)
    else:
        return (heading,)


def add_headings_path(G):
    H = hierarchy_graph(G)
    heading_paths = {}
    for key in H.nodes:
        heading_paths[key] = "/".join(get_heading_path(H, key))
    nx.set_node_attributes(G, heading_paths, "heading_path")


# def community_lookup(G, communities, node):
#     """
#     Retrieve the community to which 'node' belongs in 'G' according to the mapping specified by 'communities'.
#     """
#     if node in communities:
#         return communities[node]
#     else:
#         parents = list(G.predecessors(node))
#         if len(parents) == 0:
#             return None
#         else:
#             parent = parents[0]
#             return community_lookup(G, communities, parent)


def get_seqitems_with_community(G, result):
    # """
    # Obtain clustering result for 'G' at seqitem level, reverting the rollup used in the clustering.
    # """
    # result_extended = dict()
    # leaves = get_leaves(G)
    # for leaf in leaves:
    #     result_extended[leaf] = community_lookup(G, result, leaf)
    #     if result_extended[leaf] is None:
    #         raise Exception(leaf)
    # return result_extended

    raise Exception("Use get_clustering_result in graph_api instead.")


##########
# Plotting
##########


def plot_graphviz_tree(G, layout, attrs=dict(), root_level=None, savepath=False):
    """
    Graphviz options: dot|neato|twopi|circo|fdp|sfdp.
    Good for trees: dot|twopi|circo. The others are just pretty to look at.
    """
    helper_graph = nx.MultiDiGraph()
    helper_graph.add_nodes_from(G.nodes())
    helper_graph.add_edges_from(
        [
            (e[0], e[1])
            for e in G.edges(data=True)
            if e[-1]["edge_type"] == "containment"
        ]
    )
    root = (
        [n for n in G.nodes() if G.nodes[n]["level"] == root_level][-1]
        if root_level
        else None
    )
    pos = nx.drawing.nx_agraph.graphviz_layout(helper_graph, prog=layout, root=root)
    plt.figure()
    nG = induced_subgraph(
        G, filter_type="edge", filter_attribute="edge_type", filter_values=["reference"]
    )
    nx.draw(nG, pos, **attrs)
    if savepath:
        plt.savefig(f'{savepath}/{G.graph["name"]}.svg', transparent=True)
