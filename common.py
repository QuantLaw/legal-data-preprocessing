import argparse
import json
import multiprocessing
import os
import pickle
import re
from collections import Counter
from multiprocessing import cpu_count

import bs4
import pandas as pd
from regex import regex
from send2trash import send2trash

from statics import (
    DE_LAW_NAMES_PATH,
    DE_LAW_VALIDITIES_PATH,
    DE_LAW_NAMES_COMPILED_PATH,
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


#############
# File system
#############


def unwrap_subfolders(path, ignored_files=[], allowe_override=[]):
    """moves files from subdir of path into path"""
    subfolders = [f.name for f in os.scandir(path) if f.is_dir()]
    for subfolder in subfolders:
        error = False
        for item in os.listdir(f"{path}/{subfolder}"):
            # Skip ignored files
            if item in ignored_files:
                continue

            # Prevent overwriting files
            if os.path.exists(f"{path}/{item}") and item not in allowe_override:
                print(f"{path}/{subfolder}/{item} could not be moved")
                error = True
            else:
                os.rename(f"{path}/{subfolder}/{item}", f"{path}/{item}")

        if not error:
            send2trash(f"{path}/{subfolder}")


def ensure_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def load_json(path):
    """
    Helper to unclutter json loading.
    """
    with open(path) as f:
        return json.load(f)


def list_dir(path, type):
    return [f for f in os.listdir(path) if f.endswith(type)]


def list_dir_extended(directory, selector_func, full_path=False):
    """
    Wrapper around os.listdir to select only certain files from the directory and optionally output the full paths.
    Example usage: list_dir(path_to_directory, lambda x:x.endswith('json'), full_path=True)
    """
    if not full_path:
        return [x for x in os.listdir(directory) if selector_func(x)]
    else:
        return [f"{directory}/{x}" for x in os.listdir(directory) if selector_func(x)]


###############
# BeautifulSoup
###############


def create_soup(path):
    with open(path, encoding="utf8") as f:
        return bs4.BeautifulSoup(f.read(), "lxml-xml")


def create_html_soup(path):
    with open(path, encoding="utf8") as f:
        return bs4.BeautifulSoup(f.read(), "lxml")


def save_soup(soup, path):
    try:
        with open(path, "w") as f:
            f.write(str(soup))
    except:  # Clean file if error
        if os.path.exists(path):
            os.remove(path)
        raise


########################
# DE categorize headings
########################


heading_types = (
    "Unterabschnitt",
    "Untertitel",
    "Unterkapitel",
    "Unterartikel",
    "Abschnitt",
    "Teil",
    "Buch",
    "Kapitel",
    "Titel",
    "Art",
    "Hauptstück",
    "arabic-dot",  # 1., 2., ...
    ("roman-upper-dot", "alpha-upper-dot"),  # s.u.
    "roman-upper-dot",  # I., II., ...
    "alpha-upper-dot",  # A., B., ...
    "alpha-lower-bracket",  # a), b), ...
    #     'Anlage', # Remove for nesting
    #     'Anhang' # Remove for nesting
)


def categorize_heading_regex(heading_type):
    patterns = {
        "arabic-dot": r"\d+(\s?[a-zA-Z]{1,2})?\.",
        ("roman-upper-dot", "alpha-upper-dot"): r"(I|V|X|L)\.",
        "roman-upper-dot": r"(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})(\s?[a-z])?\.",
        "alpha-upper-dot": r"[A-Z]\.",
        "alpha-lower-bracket": r"[a-z]{1,2}\)",
    }
    if patterns.get(heading_type):
        return patterns[heading_type]

    assert "-" not in heading_type
    return r"(?:[\w\.]+\s)?(?:(?:bis|und)\s[\w\.]+\s)?" + heading_type + r"\b"


########################
# Generic Data Wrangling
########################


def extract_lawname_from_nodekey(nodekey, country_code):
    """
    :param nodekey: the key of a node in a statute graph, e.g., the crossreference graph
    :param country_code: us|de
    :return: the short identifier of the law the node belongs to
    """
    if country_code.lower() == "de":
        return nodekey.split("_")[1]
    else:
        return nodekey.split("_")[0]


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


def stem_law_name(name):
    """
    Stems name of laws to prepare for recognizing laws in the code
    """
    result = re.sub(
        r"(?<!\b)(er|en|es|s|e)(?=\b)", "", name.strip(), flags=re.IGNORECASE
    )
    return clean_name(result)


def clean_name(name):
    result = re.sub(r"\s+", " ", name)
    return (
        result.replace("ß", "ss")
        .lower()
        .replace("ä", "ae")
        .replace("ü", "ue")
        .replace("ö", "oe")
    )


def load_law_names_compiled():
    with open(DE_LAW_NAMES_COMPILED_PATH, "rb") as f:
        return pickle.load(f)


def get_stemmed_law_names_for_filename(filename, law_names):

    date = os.path.splitext(filename)[0].split("_")[1]

    laws_lookup = law_names[date]

    # Custom law names, stemmed as key.
    laws_lookup["grundgesetz"] = "GG"

    # TODO Move to compile
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


def get_snapshot_law_list(date, df=None):
    raise Exception("implement")
    if df is None:
        df = pd.read_csv(DE_LAW_VALIDITIES_PATH, index_col="filename")
    snapshot_files_df = df[
        (df["start"].fillna("") <= date)
        & ((df["end"].fillna("") == "") | (df["end"].fillna("9999-99-99") >= date))
    ]
    return list(snapshot_files_df.index)


def find_parent_with_name(tag, name):
    """
    :param tag: A tag of a BeautifulSoup
    :param name: name to search in parents
    :return: the nearest ancestor with the name
    """
    if tag.name == name:
        return tag
    else:
        return find_parent_with_name(tag.parent, name)


#####################
# Community detection
#####################


def filename_for_pp_config(
    snapshot,
    pp_ratio,
    pp_decay,
    pp_merge,
    file_ext,
    seed=None,
    markov_time=None,
    consensus=0,
    number_of_modules=None,
):
    filename = f"{snapshot}_{pp_ratio}_{pp_decay}_{pp_merge}"
    if number_of_modules:
        filename += f"_n{number_of_modules}"
    if markov_time:
        filename += f"_m{markov_time}"
    if seed is not None:
        filename += f"_s{seed}"
    if consensus:
        filename += f"_c{consensus}"
    return filename.replace(".", "-") + file_ext


def get_config_from_filename(filename):
    components = filename.split("_")
    config = dict(
        snaphot=components[0],
        pp_ratio=float(components[1].replace("-", ".")),
        pp_decay=float(components[2].replace("-", ".")),
        pp_merge=int(components[3].replace("-", ".")),
    )
    for component in components[4:]:
        if component.startswith("n"):
            config["number_of_modules"] = int(component[1:].replace("-", "."))
        if component.startswith("m"):
            config["markov_time"] = float(component[1:].replace("-", "."))
        if component.startswith("s"):
            config["seed"] = int(component[1:].replace("-", "."))
        if component.startswith("c"):
            config["consensus"] = int(component[1:].replace("-", "."))
    return config
