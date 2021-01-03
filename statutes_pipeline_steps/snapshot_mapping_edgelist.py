import json
import os
import pickle
from collections import Counter, deque
from multiprocessing import Pool

import networkx as nx
import textdistance
import tqdm
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.networkx import get_leaves
from quantlaw.utils.pipeline import PipelineStep
from regex import regex

from utils.common import get_snapshot_law_list, invert_dict_mapping_unique
from utils.string_list_contains import StringContainsAlign


class SnapshotMappingEdgelistStep(PipelineStep):
    max_number_of_processes = 1

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
        items = sorted(list_dir(self.source, ".pickle"))
        items = [i[: -len(".pickle")] for i in items]

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

        # STEP 1: perfect matches unique when considering text
        new_mappings = map_unique_texts(
            data1, data2, min_text_length=self.min_text_length
        )
        remaining_keys1, remaining_keys2 = get_remaining(
            data1["keys"], data2["keys"], new_mappings, printing=f"{item}/Step 1"
        )

        # STEP 2: perfect matches unique when considering text _and_ citekey
        new_mappings_current_step = map_same_citekey_same_text(
            data1, data2, remaining_keys1, remaining_keys2
        )
        new_mappings = {**new_mappings_current_step, **new_mappings}
        del new_mappings_current_step
        remaining_keys1, remaining_keys2 = get_remaining(
            data1["keys"], data2["keys"], new_mappings, printing=f"{item}/Step 2"
        )

        # STEP 3: text appended/prepended/removed
        new_mappings_current_step = map_text_containment(
            data1, data2, remaining_keys1, remaining_keys2
        )
        new_mappings = {**new_mappings_current_step, **new_mappings}
        del new_mappings_current_step
        remaining_keys1, remaining_keys2 = get_remaining(
            data1["keys"], data2["keys"], new_mappings, printing=f"{item}/Step 3"
        )

        # STEP 4: neighborhood matching
        data_keys1 = data1["keys"]
        data_keys2 = data2["keys"]
        data_texts1 = data1["texts"]
        data_texts2 = data2["texts"]
        del data1
        del data2

        common_neighbor_kwargs = dict(
            new_mappings=new_mappings,
            data_keys1=data_keys1,
            data_keys2=data_keys2,
            data_texts1=data_texts1,
            data_texts2=data_texts2,
            remaining_keys1=remaining_keys1,
            remaining_keys2=remaining_keys2,
            radius=self.radius,
            distance_threshold=self.distance_threshold,
        )

        text_distance_cache = map_similar_text_common_neighbors(
            **common_neighbor_kwargs,
            printing=str(item),
            dry_run=True,
        )
        text_distance_cache = update_textdistance_cache(text_distance_cache)
        map_similar_text_common_neighbors(
            **common_neighbor_kwargs,
            printing=str(item),
            text_distance_cache=text_distance_cache,
        )

        dest_path = f"{self.destination}/{mapping_filename(item)}"
        with open(dest_path, "w") as f:
            json.dump(new_mappings, f)

        # only called to print stats
        get_remaining(data_keys1, data_keys2, new_mappings, printing=f"{item}/DONE")

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


def get_remaining(data_keys1, data_keys2, new_mappings, asserting=True, printing=True):
    """
    Prints stats and returns keys of both snapshots to be matched
    """
    remaining_keys1 = set(data_keys1) - set(new_mappings.keys())
    remaining_keys2 = set(data_keys2) - set(new_mappings.values())
    if asserting:
        assert len(set(new_mappings.keys())) == len(set(new_mappings.values()))
    if printing:
        print(
            f"\n{printing}; "
            f"Progress {len(new_mappings)/min(len(data_keys1), len(data_keys2))}; "
            f"Remaining keys: {len(remaining_keys1)} {len(remaining_keys2)}; "
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
    leaf_texts1 = {k: t for k, t in zip(data1["keys"], data1["texts"])}
    leaf_texts2 = {k: t for k, t in zip(data2["keys"], data2["texts"])}

    # Create dicts with text as keys
    inverted_unique_leave_texts1 = invert_dict_mapping_unique(leaf_texts1)
    inverted_unique_leave_texts2 = invert_dict_mapping_unique(leaf_texts2)

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
    text_and_citekeys1 = {
        k: (c.lower(), t)
        for k, t, c in zip(data1["keys"], data1["texts"], data1["citekeys"])
        if c and k in remaining_keys1
    }
    text_and_citekeys2 = {
        k: (c.lower(), t)
        for k, t, c in zip(data2["keys"], data2["texts"], data2["citekeys"])
        if c and k in remaining_keys2
    }
    inverted_text_and_citekeys1 = invert_dict_mapping_unique(text_and_citekeys1)
    inverted_text_and_citekeys2 = invert_dict_mapping_unique(text_and_citekeys2)

    both_unique_text_and_citekeys = set(inverted_text_and_citekeys1.keys()) & set(
        inverted_text_and_citekeys2.keys()
    )

    # Create mapping
    new_mappings = {}
    for text_and_citekey in both_unique_text_and_citekeys:
        new_mappings[
            inverted_text_and_citekeys1[text_and_citekey]
        ] = inverted_text_and_citekeys2[text_and_citekey]
    return new_mappings


def clip_text_for_containment_matching(text):
    return text.split(" ", 1)[-1]  # get rid of German Absatz numbers (e.g., "(1)")


def map_text_containment(
    data1,
    data2,
    remaining_keys1,
    remaining_keys2,
    min_text_length=50,
):
    remaining_keys1_list = sorted(remaining_keys1)
    remaining_keys2_list = sorted(remaining_keys2)
    leave_texts1_dict = {k: t for k, t in zip(data1["keys"], data1["texts"])}
    leave_texts2_dict = {k: t for k, t in zip(data2["keys"], data2["texts"])}

    aligner = StringContainsAlign(min_text_length=min_text_length)
    aligner.text_list_0 = [
        clip_text_for_containment_matching(leave_texts1_dict[k])
        for k in remaining_keys1_list
    ]
    aligner.text_list_1 = [
        clip_text_for_containment_matching(leave_texts2_dict[k])
        for k in remaining_keys2_list
    ]
    aligner.create_index()

    containment_idx_forward = aligner.run()
    containment_idx_reversed = aligner.run(reversed=True)
    aligner.clean_index()

    containment_idx_reversed = [(v, u) for u, v in containment_idx_reversed]

    containment_idx = set(containment_idx_forward + containment_idx_reversed)

    # Filter one to one matches
    idx_1_counts = Counter(u for u, v in containment_idx)
    idx_2_counts = Counter(v for u, v in containment_idx)

    unique_keys_1 = {idx for idx, cnt in idx_1_counts.items() if cnt == 1}
    unique_keys_2 = {idx for idx, cnt in idx_2_counts.items() if cnt == 1}

    new_mappings = {}
    for u, v in containment_idx_forward + containment_idx_reversed:
        if u in unique_keys_1 and v in unique_keys_2:
            u_key = remaining_keys1_list[u]
            v_key = remaining_keys2_list[v]
            new_mappings[u_key] = v_key

    return new_mappings


def get_neighborhood(data_keys, node, radius, keys_len, key_index_dict):

    curr_index = key_index_dict[node]
    lower_bound = max(0, curr_index - radius)
    upper_bound = min(keys_len, curr_index + radius)

    neighborhood = data_keys[lower_bound : upper_bound + 1]

    # Remove node in radius but of another law/title as their order ist mostly arbitrary
    key_prefix = node.split("_")[0]
    neighborhood = {n for n in neighborhood if n.startswith(key_prefix)}

    return neighborhood


def cached_text_distance(s1, s2, cache, dry_run):
    key = (s1, s2)
    if dry_run:
        distance = None
        cache[key] = distance
    elif key not in cache:
        distance = textdistance.jaro_winkler(s1, s2)
        cache[key] = distance
    else:
        distance = cache[key]
    return distance


def calc_text_distance(args):
    return textdistance.jaro_winkler(*args)


def update_textdistance_cache(text_distance_cache):
    text_distance_texts = list(text_distance_cache.keys())
    with Pool() as p:
        distances = tqdm.tqdm(
            p.imap(calc_text_distance, text_distance_texts),
            total=len(text_distance_texts),
        )
        return {k: v for k, v in zip(text_distance_texts, distances)}


def map_similar_text_common_neighbors(
    new_mappings,
    data_keys1,
    data_keys2,
    data_texts1,
    data_texts2,
    remaining_keys1,
    remaining_keys2,
    radius=5,
    distance_threshold=0.9,
    printing=None,
    dry_run=False,
    text_distance_cache=None,
):
    if not text_distance_cache:
        text_distance_cache = dict()

    keys_len1 = len(data_keys1)
    keys_len2 = len(data_keys2)
    key_index_dict1 = {k: idx for idx, k in enumerate(data_keys1)}
    key_index_dict2 = {k: idx for idx, k in enumerate(data_keys2)}

    leave_texts1 = {k: v for k, v in zip(data_keys1, data_texts1)}
    leave_texts2 = {k: v for k, v in zip(data_keys2, data_texts2)}

    key_queue = deque(remaining_keys1)
    key_queue_set = set(key_queue)
    i = -1  # only to print the process
    while key_queue:
        remaining_key1 = key_queue.popleft()
        key_queue_set.remove(remaining_key1)
        i += 1  # only to print the process
        if i % 100 == 0 and printing:
            total = len(key_queue) + i
            print(
                f"\r{printing} " f"{i/total*100:.2f}% \t ({total} )",
                end="",
            )

        remaining_text1 = leave_texts1[remaining_key1]

        # Get neighborhood of node in G1
        # Get mapping to G2 for neighborhood nodes
        # Get neighborhood of mapped G2 nodes
        neighborhood_nodes1 = get_neighborhood(
            data_keys1, remaining_key1, radius, keys_len1, key_index_dict1
        )
        neighborhood_nodes2 = set()

        for neighborhood_node1 in neighborhood_nodes1:
            if neighborhood_node1 in new_mappings:
                neighborhood_nodes2.update(
                    get_neighborhood(
                        data_keys2,
                        new_mappings[neighborhood_node1],
                        radius,
                        keys_len2,
                        key_index_dict2,
                    )
                )

        # Remove duplicates in G2 neighborhood
        neighborhood_nodes2 = [x for x in neighborhood_nodes2 if x in remaining_keys2]

        # Find most similar text
        neighborhood_text2 = [leave_texts2.get(x) for x in neighborhood_nodes2]
        similarity = [
            cached_text_distance(remaining_text1, x, text_distance_cache, dry_run)
            if x
            else 0
            for x in neighborhood_text2
        ]
        if not dry_run:
            max_similarity = max(similarity) if similarity else 0

            if max_similarity > distance_threshold:
                # Add to mapping and update remaining_keys
                max_index = similarity.index(max_similarity)
                id2_to_match_to = neighborhood_nodes2[max_index]
                new_mappings[remaining_key1] = id2_to_match_to
                remaining_keys2.remove(id2_to_match_to)
                remaining_keys1.remove(remaining_key1)

                # Requeue neighborhood of newly mapped element
                neighborhood_to_requeue = [
                    n
                    for n in neighborhood_nodes1
                    if n in remaining_keys1 and n not in key_queue_set
                ]
                key_queue.extend(neighborhood_to_requeue)
                key_queue_set.update(neighborhood_to_requeue)

    print()
    return text_distance_cache
