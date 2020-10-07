import networkx as nx
from quantlaw.utils.networkx import hierarchy_graph

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

