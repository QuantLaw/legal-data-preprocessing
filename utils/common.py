import argparse
import os
import pickle
import shutil
from collections import Counter

import pandas as pd
from quantlaw.utils.files import ensure_exists
from regex import regex

from statics import DATA_PATH, DE_LAW_NAMES_COMPILED_PATH, DE_LAW_NAMES_PATH

##########
# Pipeline
##########


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
    Args:
        mapping_dictionary: mapping from keys to values which is not necessarily
            injective, e.g., node_id to community_id mapping

    Returns: inverted mapping with unique values as keys and lists of former keys as
        values, e.g., community_id to node_id mapping

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
