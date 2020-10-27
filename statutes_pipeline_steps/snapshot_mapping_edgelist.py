import json
import os
import pickle
from collections import Counter, deque

import networkx as nx
import textdistance
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.networkx import get_leaves
from quantlaw.utils.pipeline import PipelineStep
from regex import regex
from whoosh.index import open_dir
from whoosh.query import Phrase

from utils.common import (
    get_snapshot_law_list,
    invert_dict_mapping_all,
    invert_dict_mapping_unique,
)


class SnapshotMappingEdgelistStep(PipelineStep):
    def __init__(
        self,
        source,
        destination,
        interval,
        dataset,
        min_text_length=50,
        radius=5,
        distance_threshold=0.9,
        *args,
        **kwargs,
    ):
        self.source = source
        self.destination = destination
        self.interval = interval
        self.dataset = dataset
        self.min_text_length = min_text_length
        self.radius = radius
        self.distance_threshold = distance_threshold
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(self.destination)
        items = sorted(list_dir(self.source, "_index"))
        items = [i[: -len("_index")] for i in items]

        # Create mappings to draw the edges
        mappings = [
            (file1, file2)
            for file1, file2 in zip(items[: -self.interval], items[self.interval :])
        ]

        if snapshots:
            mappings = list(filter(lambda f: f[0] in snapshots, mappings))

        if not overwrite:
            existing_files = list_dir(self.destination, ".json")
            mappings = list(
                filter(lambda x: mapping_filename(x) not in existing_files, mappings)
            )

        return mappings

    def execute_item(self, item):
        filename1, filename2 = item

        data1 = self.load_pickle(filename1)
        data2 = self.load_pickle(filename2)

        print("Text loaded")

        # STEP 1: unique perfect matches
        new_mappings = map_unique_texts(
            data1, data2, min_text_length=self.min_text_length
        )
        remaining_keys1, remaining_keys2 = get_remaining(data1, data2, new_mappings)

        print("Step 1")

        # STEP 2: nonunique, nonmoved perfect matches
        new_mappings_current_step = map_same_citekey_same_text(
            data1, data2, remaining_keys1, remaining_keys2
        )
        new_mappings = {**new_mappings_current_step, **new_mappings}
        remaining_keys1, remaining_keys2 = get_remaining(data1, data2, new_mappings)

        print("Step 2")

        # STEP 3: text appended/prepended/removed
        index_path1 = os.path.join(self.source, filename1 + "_index")
        index_path2 = os.path.join(self.source, filename2 + "_index")
        new_mappings_current_step = map_text_containment(
            index_path1, index_path2, data1, data2, remaining_keys1, remaining_keys2
        )
        new_mappings = {**new_mappings_current_step, **new_mappings}
        remaining_keys1, remaining_keys2 = get_remaining(data1, data2, new_mappings)

        print("Step 3")

        # STEP 4: neighborhood matching
        map_similar_text_common_neighbors(
            new_mappings,
            data1,
            data2,
            remaining_keys1,
            remaining_keys2,
            radius=self.radius,
            distance_threshold=self.distance_threshold,
        )

        print("Step 4")

        with open(f"{self.destination}/{mapping_filename(item)}", "w") as f:
            json.dump(new_mappings, f)

        # only called to print stats
        get_remaining(data1, data2, new_mappings)

    def load_pickle(self, snapshot):
        with open(os.path.join(self.source, snapshot + ".pickle"), "rb") as f:
            raw_data = pickle.load(f)
        return raw_data


def mapping_filename(mapping):
    """
    returns the filename mappings are stored in
    """
    filename1, filename2 = mapping
    result = f"{filename1}_{filename2}.json"
    return result


def load_crossref_graph(filename, source):
    graph_path = f"{source}/{filename}"
    G = nx.read_gpickle(graph_path)
    return G


def get_remaining(data1, data2, new_mappings, asserting=True, printing=True):
    """
    Prints stats and returns keys of both snapshots to be matched
    """
    remaining_keys1 = set(data1["keys"]) - set(new_mappings.keys())
    remaining_keys2 = set(data2["keys"]) - set(new_mappings.values())
    if asserting:
        assert len(set(new_mappings.keys())) == len(set(new_mappings.values()))
    if printing:
        print(f"{len(remaining_keys1)} {len(remaining_keys2)}")
        print(
            f"Progress {len(new_mappings)/min(len(data1['keys']), len(data2['keys']))}"
        )
    return remaining_keys1, remaining_keys2


def get_leaf_texts_to_compare(
    graph_filename, G, source_text, source_text_reg, law_names_data, dataset
):
    """
    get text for leaves of a hierarchy graph. Can be seqitem or supseqitem graph.
    Leaves are only seqitems or supseqitems.
    """
    leaf_keys = get_leaves(G)

    snapshot = graph_filename[: -len(".gpickle.gz")]

    if dataset == "us":
        files = [
            os.path.join(source_text, x)
            for x in list_dir(source_text, ".xml")
            if x.split(".")[0].split("_")[-1] == snapshot
        ]
        if source_text_reg:
            files += [
                os.path.join(source_text_reg, x)
                for x in list_dir(source_text_reg, ".xml")
                if x.split(".")[0].split("_")[-1] == snapshot
            ]
        files.sort()
    else:  # is DE
        files = get_snapshot_law_list(snapshot, law_names_data)
        files = [os.path.join(source_text, f) for f in files]

    whitespace_pattern = regex.compile(r"[\s\n]+")
    texts = {}
    for file in files:
        print(f"\r{files.index(file)} / {len(files)}", end="")
        soup = create_soup(file)
        tags = soup.find_all(["seqitem", "subseqitem"])
        for tag in tags:
            if tag["key"] in leaf_keys:
                text = tag.get_text(" ")
                text = whitespace_pattern.sub(" ", text).lower().strip()
                texts[tag["key"]] = text.lower()
    return texts


def map_unique_texts(data1, data2, min_text_length=50):
    """
    Maps nodes from snapshot t1 to t2 if texts are in each snapshot unique and appear
    in the both snapshots
    """
    leave_texts1 = {k: t for k, t in zip(data1["keys"], data1["texts"])}
    leave_texts2 = {k: t for k, t in zip(data2["keys"], data2["texts"])}

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


def map_same_citekey_same_text(data1, data2, remaining_keys1, remaining_keys2):
    # inverted_leave_texts1 = invert_dict_mapping_all(leave_texts1)
    # # currently not needed
    leave_texts1 = {k: t for k, t in zip(data1["keys"], data1["texts"])}
    leave_texts2 = {k: t for k, t in zip(data2["keys"], data2["texts"])}

    cite_keys1 = {k: c for k, c in zip(data1["keys"], data1["citekeys"])}
    cite_keys2 = {k: c for k, c in zip(data2["keys"], data2["citekeys"])}

    inverted_leave_texts2 = invert_dict_mapping_all(leave_texts2)

    new_mappings = {}

    for remaining_key1 in sorted(remaining_keys1):
        text1 = leave_texts1[remaining_key1]
        ids2 = inverted_leave_texts2.get(text1)
        if not ids2 or not len(ids2):
            continue
        cite_key1 = cite_keys1[remaining_key1]
        # does not work for subseqitems. in this case to up to seqitem and use their
        # citekey. Same for cite_key2.
        if not cite_key1:
            continue
        for id2 in ids2:
            cite_key2 = cite_keys2[id2]
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
    index_path1,
    index_path2,
    data1,
    data2,
    remaining_keys1,
    remaining_keys2,
    min_text_length=50,
):
    leave_texts1 = {k: v for k, v in zip(data1["keys"], data1["texts"])}
    leave_texts2 = {k: v for k, v in zip(data2["keys"], data2["texts"])}

    candidate_mappings = {}

    matched_keys2 = Counter()
    matched_keys1 = Counter()

    ix = open_dir(index_path1)
    i = 0
    with ix.searcher() as searcher:
        for remaining_key1 in sorted(remaining_keys1):
            i += 1
            print(f"\r{i} / {len(remaining_keys1)}", end="")
            text_clipped = clip_text_for_containment_matching(
                leave_texts1[remaining_key1]
            )
            if len(text_clipped) > min_text_length:
                query_text = text_clipped.lower()
                query = Phrase("content", query_text.split())
                results = searcher.search(query)
                result_keys = [r["key"] for r in results]
                result_keys = [k for k in result_keys if k in remaining_keys2]
                matched_keys2.update(result_keys)
                if len(result_keys) == 1:
                    candidate_mappings[remaining_key1] = result_keys[0]

    ix = open_dir(index_path2)
    with ix.searcher() as searcher:
        for remaining_key2 in sorted(remaining_keys2):
            text_clipped = clip_text_for_containment_matching(
                leave_texts2[remaining_key2]
            )
            if len(text_clipped) <= min_text_length:
                query_text = text_clipped.lower()
                query = Phrase("content", query_text.split())
                results = searcher.search(query)
                result_keys = [r["key"] for r in results]
                result_keys = [k for k in result_keys if k in remaining_keys1]
                matched_keys1.update(result_keys)
                if len(result_keys) == 1:
                    candidate_mappings[result_keys[0]] = remaining_key2

    new_mappings = {
        key1: key2
        for key1, key2 in candidate_mappings.items()
        if matched_keys1[key1] == 1 and matched_keys2[key2] == 1
    }

    return new_mappings


def get_neighborhood(data, node, radius):

    curr_index = data["keys"].index(node)
    lower_bound = max(0, curr_index - radius)
    upper_bound = min(len(data["keys"]), curr_index + radius)

    neighborhood = data["keys"][lower_bound : upper_bound + 1]

    # Remove node in radius but of another law/title as their order ist mostly arbitrary
    key_prefix = node.split("_")[0]
    neighborhood = [n for n in neighborhood if n.startswith(key_prefix)]

    return neighborhood


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
    data1,
    data2,
    remaining_keys1,
    remaining_keys2,
    radius=5,
    distance_threshold=0.9,
):
    leave_texts1 = {k: v for k, v in zip(data1["keys"], data1["texts"])}
    leave_texts2 = {k: v for k, v in zip(data2["keys"], data2["texts"])}

    text_distance_cache = dict()

    key_queue = deque(remaining_keys1)  # to show progress
    i = -1  # only to print the process
    while len(key_queue):
        remaining_key1 = key_queue.popleft()
        i += 1  # only to print the process
        print(
            f"\r{i} \tof {len(key_queue) + i}",
            end="",
        )

        remaining_text1 = leave_texts1[remaining_key1]

        # Get neighborhood of node in G1
        # Get mapping to G2 for neighborhood nodes
        # Get neighborhood of mapped G2 nodes
        neighborhood_nodes1 = get_neighborhood(data1, remaining_key1, radius)
        neighborhood_nodes2 = []

        for neighborhood_node1 in neighborhood_nodes1:
            if neighborhood_node1 in new_mappings:
                neighborhood_nodes2 += get_neighborhood(
                    data2, new_mappings[neighborhood_node1], radius
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
