import itertools
import os

import bs4
from regex import regex

from legal_data_common_utils.common import (
    ensure_exists,
    list_dir,
    create_soup,
    save_soup,
)
from statics import (
    US_REFERENCE_AREAS_PATH,
    US_XML_PATH,
    US_REFERENCE_AREAS_LOG_PATH,
    US_HELPERS_PATH,
)


def us_reference_areas_prepare(overwrite):
    ensure_exists(US_REFERENCE_AREAS_PATH)
    files = list_dir(US_XML_PATH, ".xml")

    if not overwrite:
        existing_files = os.listdir(US_REFERENCE_AREAS_PATH)
        files = list(filter(lambda f: f not in existing_files, files))

    return files


def us_reference_areas(filename):
    soup = create_soup(f"{US_XML_PATH}/{filename}")
    logs = find_references(soup, usc_pattern, {"pattern": "block"})
    logs += find_references(soup, inline_pattern, {"pattern": "inline"})
    save_soup(soup, f"{US_REFERENCE_AREAS_PATH}/{filename}")
    return logs


def us_reference_areas_finish(logs_per_file):
    logs = list(itertools.chain.from_iterable(logs_per_file))
    ensure_exists(US_HELPERS_PATH)
    with open(US_REFERENCE_AREAS_LOG_PATH, mode="w") as f:
        f.write("\n".join(sorted(logs, key=lambda x: x.lower())))


################
# Regex patterns
################

# fmt: off

regex_definitions = (
    r'(?(DEFINE)'
        r'(?<sec>'
            r'(\d+[\da-zA-Z\-\–\—\.]*)(?<!\.)'
            r'(\(\d*[a-z]{0,3}i*\))*'
            r'(\s+et\.?\s+seq\.?)?'
            r'(\s+and\sfollowing)?'
        r')'
        r'(?<numb>'
            r'(\(\d*[a-z]{0,2}i?\))+'
            r'(\s+et\.?\s+seq\.?)?'
        r')'
        r'(?<conn>'
            r',?\s+(and|or|to|through)\s+|'
            r'(,|;)\s+'
        r')'
    r')'
)

usc_pattern_string = (
    regex_definitions +
    r'('
        r'(\d+)\s*'
        r'U\.?S\.?C\.?\s*'
        r'(Sec(?:tion|\.)?|§)?\s*'
        r'(?&sec)'
        r'((?&conn)(Sec(?:tion|\.)?|§)?\s*(?&sec)|(?&conn)(?&numb))*'
    r')'
    r'(?!\w*(\sApp\.)?\s(U\.?S\.?C\.?|C\.?F\.?R\.?|Stat\.))'
)

# TODO LATER get other inline references e.g. chapter xx of Title 23

inline_pattern_string = (
    regex_definitions +
    r'(Sec(?:tion|\.)?|§)\s*'
    r'(?&sec)'
    r'('
        r'(?&conn)'
        r'(Sec(?:tion|\.)?|§)?'
        r'\s*'
        r'(?&sec)|(?&conn)(?&numb)'
    r')*'
    r'\s*'
    r'('
        r'(of\sthis\stitle)'
    r'|'
        r'(of\stitle\s\d+)'
    r')'
)

# fmt: on

usc_pattern = regex.compile(usc_pattern_string, flags=regex.IGNORECASE)

inline_pattern = regex.compile(inline_pattern_string, flags=regex.IGNORECASE)

###########
# Functions
###########


def add_tag(string, pos, end, tag):
    """Wraps part of a string a given tag"""
    tag.string = string[pos:end]
    return [
        bs4.element.NavigableString(string[:pos]),
        tag,
        bs4.element.NavigableString(string[end:]),
    ]


def find_references(soup, pattern, attrs):
    logs = []  # For debug

    text_tags = list(soup.find_all("text"))
    for text_tag in text_tags:
        for text_tag_string in list(text_tag.contents):
            if type(text_tag_string) is not bs4.element.NavigableString:
                continue
            tag_cursor = text_tag_string
            last_match_end = 0
            matches = pattern.finditer(text_tag_string)
            for match in list(matches):
                ref_tag = soup.new_tag("reference", **attrs)
                pre_text, ref_tag, post_text = add_tag(
                    text_tag_string, match.start(), match.end(), ref_tag
                )

                pre_text = pre_text[last_match_end:]
                last_match_end = match.end()

                tag_cursor.replace_with(ref_tag)
                ref_tag.insert_before(pre_text)
                ref_tag.insert_after(post_text)
                tag_cursor = post_text

                logs.append(f"{post_text[:50]} --- {match[0]}")  # For debug

    return logs  # For debug
