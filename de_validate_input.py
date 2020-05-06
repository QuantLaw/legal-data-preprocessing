import os
import re
import shutil

from common import ensure_exists, list_dir
from statics import (
    DE_ORIGINAL_PATH,
    DE_INPUT_PATH,
    DE_INPUT_LIST_PATH,
    DE_ORIGINAL_VERSION_INDICES_PATH,
)
import pandas as pd


def get_nr_abk_dict(remove_special_chars):
    df = pd.read_csv(DE_INPUT_LIST_PATH)
    return_dict = {}
    check_dict = {}
    for idx, row in df.iterrows():

        if remove_special_chars:
            abk = (
                row["abk"]
                .strip()
                .replace(" ", "-")
                .replace("/", "-")
                .replace("§", "-")
                .replace(".", "")
                .replace("_", "-")
                .replace("(", "-")
                .replace(")", "-")
                .replace(",", "-")
                .replace("=", "-")
            )
        else:
            abk = row["abk"]

        assert not return_dict.get(row["nr"])
        if check_dict.get(abk.lower()):
            print(f'Duplicate {abk}: {row["nr"]} - {check_dict.get(abk.lower())}')

        return_dict[row["nr"]] = abk
        check_dict[abk.lower()] = row["nr"]
    return return_dict


def de_validate_input():
    ensure_exists(DE_ORIGINAL_PATH)
    ensure_exists(DE_ORIGINAL_VERSION_INDICES_PATH)
    nr_abk_dict = get_nr_abk_dict(True)

    test_pattern = re.compile(r"[A-Za-z0-9\-äüöÖÜÄß]+")

    for nr, abk in nr_abk_dict.items():
        abk = (
            abk.strip()
            .replace(" ", "-")
            .replace("/", "-")
            .replace("§", "-")
            .replace(".", "")
            .replace("_", "-")
            .replace("(", "-")
            .replace(")", "-")
            .replace(",", "-")
            .replace("=", "-")
        )
        if not test_pattern.fullmatch(abk):
            raise Exception(f"{abk} has a unsuitable characters for a shortname")

    files = list_dir(DE_INPUT_PATH, ".html")

    for file in files:
        try:
            gesamtausgaben, nr, version = parse_juris_filenames(file)
        except JurisFilenameParseException as err:  # TODO
            print(err)
            continue

        if gesamtausgaben:
            new_filepath = (
                f"{DE_ORIGINAL_VERSION_INDICES_PATH}/{nr}_{nr_abk_dict[nr]}.html"
            )
        else:
            new_filepath = f"{DE_ORIGINAL_PATH}/{nr}_{nr_abk_dict[nr]}_{version or '99999999'}.html"
        # print(file)
        assert not os.path.exists(
            new_filepath
        ), f"{new_filepath} referenced in file {file}"
        assert "%40" not in new_filepath
        print(f"{DE_INPUT_PATH}/{file}", new_filepath)
        shutil.copy(f"{DE_INPUT_PATH}/{file}", new_filepath)


################
# Validate input
################


class JurisFilenameParseException(Exception):
    pass


def parse_juris_filenames(filename):
    try:
        _ = parse_juris_filenames.pattern
    except AttributeError:
        # fmt: off
        pattern_str = (
            r'([A-Z]+[0-9]+)-gesamtausgaben\.html|'
            r'([A-Z]+[0-9]+)-([A-Z]+[0-9]+)\.html|'
            r'([A-Z]+[0-9]+)-aiz-([A-Z]+[0-9]+)(?:@|\%40)(\d+)\.html'
        )
        # fmt: on
        parse_juris_filenames.pattern = re.compile(pattern_str)

    match = parse_juris_filenames.pattern.fullmatch(filename)

    if not match:
        raise JurisFilenameParseException(filename + " - no match")

    gesamtausgaben, nr, version = False, None, None
    if match[1]:
        nr = match[1]
        gesamtausgaben = True
    elif match[2]:
        if not match[2] == match[3]:
            raise JurisFilenameParseException(filename + " - validation failed")
        nr = match[2]
    elif match[4]:
        assert match[4] == match[5]
        nr = match[4]
        version = match[6]

    return gesamtausgaben, nr, version
