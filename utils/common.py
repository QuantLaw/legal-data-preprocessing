import argparse
import multiprocessing
import os
import pickle
import shutil
from collections import Counter
from multiprocessing import cpu_count

import pandas as pd
from quantlaw.utils.files import ensure_exists
from regex import regex

from statics import (
    DE_LAW_NAMES_PATH,
    DE_LAW_NAMES_COMPILED_PATH,
    DATA_PATH,
)


##########
# Pipeline
##########


def process_items(
    items,
    selected_items,
    action_method,
    use_multiprocessing,
    args=[],
    chunksize=None,
    processes=None,
    spawn=False,
):
    if len(selected_items) > 0:
        filtered_items = []
        for item in list(items):
            for selected_item in selected_items:
                if selected_item in item:
                    filtered_items.append(item)
                    break
        items = filtered_items
    if not processes:
        processes = int(cpu_count() - 2)
    if use_multiprocessing and len(items) > 1:
        if spawn:
            ctx = multiprocessing.get_context("spawn")
        else:
            ctx = multiprocessing.get_context()
            # A bit slower, but it reimports everything which is necessary to make matplotlib working.
            # Chunksize should be higher or none
        with ctx.Pool(processes=processes) as p:
            logs = p.starmap(action_method, [(i, *args) for i in items], chunksize)
    else:
        logs = []
        for item in items:
            logs.append(action_method(item, *args))

    return logs


def str_to_bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


########################
# Generic Data Wrangling
########################


def invert_dict_mapping_all(mapping_dictionary):
    """
    :param mapping_dictionary: mapping from keys to values which is not necessarily injective, e.g., node_id to community_id mapping
    :return: inverted mapping with unique values as keys and lists of former keys as values, e.g., community_id to node_id mapping
    """
    inverted = {v: [] for v in mapping_dictionary.values()}
    for k, v in mapping_dictionary.items():
        inverted[v].append(k)
    return inverted


def invert_dict_mapping_unique(source_dict):
    """
    Inverts keys and values of a dict. Only entries with unique values are inverted.
    """
    counter = Counter(source_dict.values())
    unique = set([text for text, cnt in counter.most_common() if cnt == 1])
    return {v: k for k, v in source_dict.items() if v in unique}


####################
# DE Crossreferences
####################


def load_law_names():
    df = pd.read_csv(DE_LAW_NAMES_PATH)
    data = [
        dict(
            citename=row.citename,
            citekey=row.citekey,
            start=row.filename.split("_")[2],
            end=os.path.splitext(row.filename)[0].split("_")[3],
            filename=row.filename,
        )
        for i, row in df.iterrows()
    ]
    return data


def load_law_names_compiled():
    with open(DE_LAW_NAMES_COMPILED_PATH, "rb") as f:
        return pickle.load(f)


def get_stemmed_law_names_for_filename(filename, law_names):
    date = os.path.splitext(filename)[0].split("_")[2]
    return get_stemmed_law_names(date, law_names)


def get_stemmed_law_names(date, law_names):
    laws_lookup = law_names[date]

    # Custom law names, stemmed as key.
    laws_lookup["grundgesetz"] = "GG"

    # Add law names without year number if key already used
    shortened_keys = {}
    for key, value in laws_lookup.items():
        match = regex.fullmatch(r"(.+)\s\d{4}[\-\d]*", key)
        if match:
            if match[1] not in shortened_keys:
                shortened_keys[match[1]] = set()
            shortened_keys[match[1]].update([value])

    for key, values in shortened_keys.items():
        if len(values) == 1 and key not in laws_lookup.keys():
            laws_lookup[key] = list(values)[0]

    return laws_lookup


def get_snapshot_law_list(date, law_names_data):
    date = date.replace("-", "")
    law_names_list = {
        d["filename"] for d in law_names_data if d["start"] <= date and d["end"] >= date
    }
    assert len(law_names_list) == len({x.split("_")[0] for x in law_names_list})
    return law_names_list


def copy_xml_schema_to_data_folder():
    ensure_exists(DATA_PATH)
    shutil.copyfile("xml-schema.xsd", os.path.join(DATA_PATH, "xml-schema.xsd"))
