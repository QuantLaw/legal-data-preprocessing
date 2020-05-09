import networkx as nx


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
        nodes = [
            n for n in G.nodes() if G.nodes[n].get(filter_attribute) in filter_values
        ]
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


####################################
# Rollup Helpers and Rolled-Up Graph
####################################


def get_leaves(G):
    H = hierarchy_graph(G)
    return set([node for node in H.nodes if H.out_degree(node) == 0])
