import json
import os
from collections import deque

import networkx as nx
import textdistance
from regex import regex


from utils.common import (
    ensure_exists,
    list_dir,
    create_soup,
    invert_dict_mapping_all,
    invert_dict_mapping_unique,
    get_snapshot_law_list,
)

import sys

sys.path.append("../")
from utils import graph_api


def mapping_filename(mapping):
    """
    returns the filename mappings are stored in
    """
    filename1, filename2 = mapping
    result = (
        f"{filename1[: -len('.gpickle.gz')]}_{filename2[: -len('.gpickle.gz')]}.json"
    )
    return result


def snapshot_mapping_edgelist_prepare(
    overwrite, snapshots, source_graph, source_text, destination, interval
):
    ensure_exists(destination)
    files = sorted(list_dir(source_graph, ".gpickle.gz"))

    # Create mappings to draw the edges
    mappings = [
        (file1, file2) for file1, file2 in zip(files[:-interval], files[interval:])
    ]

    if snapshots:
        mappings = list(
            filter(lambda f: f[0][: -len(".gpickle.gz")] in snapshots, mappings)
        )

    if not overwrite:
        existing_files = list_dir(destination, ".json")
        mappings = list(
            filter(lambda x: mapping_filename(x) not in existing_files, mappings)
        )

    return mappings


def snapshot_mapping_edgelist(
    mapping,
    source_graph,
    source_text,
    destination,
    law_names_data,
    min_text_length=50,
    radius=5,
    distance_threshold=0.9,
):
    filename1, filename2 = mapping

    G1 = load_crossref_graph(filename1, source_graph)
    G2 = load_crossref_graph(filename2, source_graph)

    leave_texts1 = get_leaf_texts_to_compare(filename1, G1, source_text, law_names_data)
    leave_texts2 = get_leaf_texts_to_compare(filename2, G2, source_text, law_names_data)

    # STEP 1: unique perfect matches
    new_mappings = map_unique_texts(
        leave_texts1, leave_texts2, min_text_length=min_text_length
    )
    remaining_keys1, remaining_keys2 = get_remaining(
        leave_texts1, leave_texts2, new_mappings
    )

    # STEP 2: nonunique, nonmoved perfect matches
    new_mappings_current_step = map_same_citekey_same_text(
        leave_texts1, leave_texts2, G1, G2, remaining_keys1, remaining_keys2
    )
    new_mappings = {**new_mappings_current_step, **new_mappings}
    remaining_keys1, remaining_keys2 = get_remaining(
        leave_texts1, leave_texts2, new_mappings
    )

    # STEP 3: text appended/prepended/removed
    new_mappings_current_step = map_text_containment(
        leave_texts1, leave_texts2, remaining_keys1, remaining_keys2
    )
    new_mappings = {**new_mappings_current_step, **new_mappings}
    remaining_keys1, remaining_keys2 = get_remaining(
        leave_texts1, leave_texts2, new_mappings
    )

    # For DEBUG
    # with open('test.txt', 'w') as f:
    #     for old, new in new_mappings_temp.items():
    #         f.write(f'{old} - {new}\n')
    #         f.write(t1[old]+'\n')
    #         f.write(t2[new]+'\n\n')

    # STEP x: neighborhood matching
    map_similar_text_common_neighbors(
        new_mappings,
        leave_texts1,
        leave_texts2,
        G1,
        G2,
        remaining_keys1,
        remaining_keys2,
        radius=radius,
        distance_threshold=distance_threshold,
    )

    with open(f"{destination}/{mapping_filename(mapping)}", "w") as f:
        json.dump(new_mappings, f)

    remaining_keys1, remaining_keys2 = get_remaining(
        leave_texts1, leave_texts2, new_mappings, asserting=False, printing=True
    )  # can be deleted, only called for stats


def load_crossref_graph(filename, source):
    graph_path = f"{source}/{filename}"
    G = nx.read_gpickle(graph_path)
    return G


def get_remaining(t1, t2, new_mappings, asserting=True, printing=False):
    """
    Prints stats and returns keys of both snapshots to be matched
    """
    remaining_keys1 = set(t1.keys()) - set(new_mappings.keys())
    remaining_keys2 = set(t2.keys()) - set(new_mappings.values())
    if asserting:
        assert len(set(new_mappings.keys())) == len(set(new_mappings.values()))
    if printing:
        print(f"{len(remaining_keys1)} {len(remaining_keys2)}")
        print(f"Progress {len(new_mappings)/min(len(t1), len(t2))}")
    return remaining_keys1, remaining_keys2


def get_leaf_texts_to_compare(graph_filename, G, source_text, law_names_data):
    """
    get text for leaves of a hierarchy graph. Can be seqitem or supseqitem graph.
    Leaves are only seqitems or supseqitems.
    """
    leaf_keys = graph_api.get_leaves(G)

    snapshot = graph_filename[: -len(".gpickle.gz")]

    if len(snapshot) == 4:  # is US
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
    texts = {}
    for file in files:
        soup = create_soup(f"{source_text}/{file}")
        tags = soup.find_all(["seqitem", "subseqitem"])
        for tag in tags:
            if tag["key"] in leaf_keys:
                text = tag.get_text(" ")
                text = whitespace_pattern.sub(" ", text).lower().strip()
                texts[tag["key"]] = text.lower()
    return texts


def map_unique_texts(leave_texts1, leave_texts2, min_text_length=50):
    """
    Maps nodes from snapshot t1 to t2 if texts are in each snapshot unique and appear in the both snapshots
    """
    # Create dicts with text as keys
    inverted_unique_leave_texts1 = invert_dict_mapping_unique(leave_texts1)
    inverted_unique_leave_texts2 = invert_dict_mapping_unique(leave_texts2)

    # find unique texts in both snapshots
    both_unique_texts = set(inverted_unique_leave_texts1.keys()) & set(
        inverted_unique_leave_texts2.keys()
    )

    # Filter for texts with min length
    both_unique_texts = {x for x in both_unique_texts if len(x) >= min_text_length}

    # Create mapping
    new_mappings = {}
    for text in both_unique_texts:
        new_mappings[inverted_unique_leave_texts1[text]] = inverted_unique_leave_texts2[
            text
        ]
    return new_mappings


def map_same_citekey_same_text(
    leave_texts1, leave_texts2, G1, G2, remaining_keys1, remaining_keys2
):
    # inverted_leave_texts1 = invert_dict_mapping_all(leave_texts1) # currently not needed
    inverted_leave_texts2 = invert_dict_mapping_all(leave_texts2)

    new_mappings = {}

    for remaining_key1 in sorted(remaining_keys1):
        text1 = leave_texts1[remaining_key1]
        ids2 = inverted_leave_texts2.get(text1)
        if not ids2 or not len(ids2):
            continue
        cite_key1 = G1.nodes[remaining_key1].get("citekey")
        # does not work for subseqitems. in this case to up to seqitem and use their citekey. Same for cite_key2.
        if not cite_key1:
            continue
        for id2 in ids2:
            cite_key2 = G2.nodes[id2].get("citekey")
            if not cite_key2:
                continue
            if cite_key1.lower() == cite_key2.lower() and id2 in remaining_keys2:
                new_mappings[remaining_key1] = id2
                remaining_keys2.remove(id2)
                break

    return new_mappings


def clip_text_for_containment_matching(text):
    return text.split(" ", 1)[-1]  # get rid of German Absatz numbers (e.g., "(1)")


def map_text_containment(
    leave_texts1, leave_texts2, remaining_keys1, remaining_keys2, min_text_length=50
):
    leave_texts_clipped1 = {
        k: clip_text_for_containment_matching(v) for k, v in leave_texts1.items()
    }
    leave_texts_clipped2 = {
        k: clip_text_for_containment_matching(v) for k, v in leave_texts2.items()
    }

    candidate_mappings = {k: list() for k in remaining_keys1}
    candidate_inverted_mappings = {k: list() for k in remaining_keys2}
    for remaining_key1 in sorted(remaining_keys1):
        text1 = leave_texts_clipped1[remaining_key1]
        for remaining_key2 in sorted(remaining_keys2):
            text2 = leave_texts_clipped2[remaining_key2]
            if (len(text1) > min_text_length and text1 in text2) or (
                len(text2) > min_text_length and text2 in text1
            ):
                candidate_mappings[remaining_key1].append(remaining_key2)
                candidate_inverted_mappings[remaining_key2].append(remaining_key1)

    candidate_mappings = {k: v[0] for k, v in candidate_mappings.items() if len(v) == 1}
    candidate_inverted_mappings = {
        k: v[0] for k, v in candidate_inverted_mappings.items() if len(v) == 1
    }

    new_mappings = {
        k: v
        for k, v in candidate_mappings.items()
        if v in candidate_inverted_mappings.keys()
    }

    return new_mappings


def get_neighborhood(G, node, radius):
    return sorted(
        nx.ego_graph(G, node, radius).nodes,
        key=lambda x: nx.shortest_path_length(G, node, x),
    )


def cached_text_distance(s1, s2, cache):
    key = (s1, s2)
    if key not in cache:
        distance = textdistance.jaro_winkler(s1, s2)
        cache[key] = distance
    else:
        distance = cache[key]
    return distance


def map_similar_text_common_neighbors(
    new_mappings,
    leave_texts1,
    leave_texts2,
    G1,
    G2,
    remaining_keys1,
    remaining_keys2,
    radius=5,
    distance_threshold=0.9,
):
    sG1 = graph_api.sequence_graph(G1)
    sG2 = graph_api.sequence_graph(G2)

    text_distance_cache = dict()

    key_queue = deque(remaining_keys1)  # to show progress
    i = -1  # only to print the process
    while len(key_queue):
        remaining_key1 = key_queue.popleft()
        i += 1  # only to print the process
        print(
            f"\r{i} \tof {len(key_queue) + i}", end="",
        )

        remaining_text1 = leave_texts1[remaining_key1]

        # Get neighborhood of node in G1
        # Get mapping to G2 for neighborhood nodes
        # Get neighborhood of mapped G2 nodes
        neighborhood_nodes1 = get_neighborhood(sG1, remaining_key1, radius)
        neighborhood_nodes2 = []

        for neighborhood_node1 in neighborhood_nodes1:
            if neighborhood_node1 in new_mappings:
                neighborhood_nodes2 += get_neighborhood(
                    sG2, new_mappings[neighborhood_node1], radius
                )

        # Remove duplicates in G2 neighborhood
        neighborhood_nodes2 = [
            x for x in set(neighborhood_nodes2) if x in remaining_keys2
        ]

        # Find most similar text
        neighborhood_text2 = [
            leave_texts2[x] if x in leave_texts2 else None for x in neighborhood_nodes2
        ]
        similarity = [
            cached_text_distance(remaining_text1, x, text_distance_cache) if x else 0
            for x in neighborhood_text2
        ]

        if len(similarity) and max(similarity) > distance_threshold:
            # Add to mapping and update remaining_keys
            max_index = similarity.index(max(similarity))
            id2_to_match_to = neighborhood_nodes2[max_index]
            new_mappings[remaining_key1] = id2_to_match_to
            remaining_keys2.remove(id2_to_match_to)
            remaining_keys1.remove(remaining_key1)

            # Requeue neighborhood of newly mapped element
            neighborghood_to_requeue = [
                n
                for n in neighborhood_nodes1
                if n in remaining_keys1 and n not in key_queue
            ]
            key_queue.extend(neighborghood_to_requeue)

    #         print(max(similarity))
    #         print(remaining_text1)
    #         print('-')
    #         print(neighborhood_text2[max_index])
    #         print('=====\n')
