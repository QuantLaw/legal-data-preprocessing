import itertools
import os

import bs4
from quantlaw.de_extract.statutes_abstract import StatutesMatchWithMainArea
from quantlaw.de_extract.statutes_areas import StatutesExtractor
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists, list_dir

from utils.common import (
    get_stemmed_law_names_for_filename,
)
from statics import (
    DE_REFERENCE_AREAS_PATH,
    DE_HELPERS_PATH,
    DE_REFERENCE_AREAS_LOG_PATH,
    DE_XML_PATH,
)


def de_reference_areas_prepare(overwrite):
    ensure_exists(DE_REFERENCE_AREAS_PATH)
    files = list_dir(DE_XML_PATH, ".xml")

    if not overwrite:
        existing_files = os.listdir(DE_REFERENCE_AREAS_PATH)
        files = list(filter(lambda f: f not in existing_files, files))
    return files


def de_reference_areas(filename, law_names):
    laws_lookup = get_stemmed_law_names_for_filename(filename, law_names)
    extractor = StatutesExtractor(laws_lookup)
    logs = []
    soup = create_soup(f"{DE_XML_PATH}/{filename}")
    para, art, misc = analyze_type_of_headings(soup)


    logs.extend(find_references_in_soup(soup, extractor, para, art))

    # Find references without preceding article or § (currently not implemented)
    # long_law_regex_pattern = law_keys_to_regex(laws_lookup_keys, 5)
    # short_law_regex_pattern = law_keys_to_regex(laws_lookup_keys, 3, 4)
    # for section in soup.find_all("text"):
    #     find_law_references_in_section(
    #         section, soup, long_law_regex_pattern, stem_law_name
    #     )
    #     find_law_references_in_section(
    #         section, soup, short_law_regex_pattern, clean_name
    #     )

    save_soup_with_style(soup, f"{DE_REFERENCE_AREAS_PATH}/{filename}")

    return logs


def de_reference_areas_finish(logs_per_file):
    logs = list(itertools.chain.from_iterable(logs_per_file))
    ensure_exists(DE_HELPERS_PATH)
    with open(DE_REFERENCE_AREAS_LOG_PATH, mode="w") as f:
        f.write("\n".join(sorted(logs, key=lambda x: x.lower())))


########################################
# Functions general and normal citations
########################################


def save_soup_with_style(soup, path):
    output_lines = str(soup).replace("\n\n", "\n").split("\n")
    output_lines.insert(1, '<?xml-stylesheet href="../../notebooks/xml-styles.css"?>')
    output = "\n".join(output_lines)

    with open(path, "w") as f:
        f.write(output)


def analyze_type_of_headings(soup):
    para = 0
    art = 0
    misc = 0
    for tag in soup.find_all("seqitem"):
        if "heading" not in tag.attrs:
            misc += 1
        elif tag.attrs["heading"].replace("\n", "").startswith("§"):
            para += 1
        elif tag.attrs["heading"].replace("\n", "").lower().startswith("art"):
            art += 1
        else:
            misc += 1
    return para, art, misc


def add_tag(string, pos, end, tag):
    tag.string = string[pos:end]
    return [
        bs4.element.NavigableString(string[:pos]),
        tag,
        bs4.element.NavigableString(string[end:]),
    ]


def split_reference(string, len_main, len_suffix, soup):
    main_str = string[:len_main]
    suffix_str = string[len_main : len_main + len_suffix]
    law_str = string[len_main + len_suffix :]

    result = [soup.new_tag("main"), soup.new_tag("suffix"), soup.new_tag("lawname")]
    result[0].append(main_str)
    result[1].append(suffix_str)
    result[2].append(law_str)

    return result


def handle_reference_match(
    match: StatutesMatchWithMainArea, section, soup, para, art
):
    # Set internal references to ignore if seqitem unit (Art|§) does not match between reference and target law
    if match.law_match_type == "internal":
        if (section.contents[-1][match.start:].startswith("§") and para == 0) or (
            section.contents[-1][match.start:].lower().startswith("art") and art == 0
        ):
            law_match_type = "ignore"

    ref_tag = soup.new_tag("reference", pattern="inline")
    section.contents[-1:] = add_tag(
        section.contents[-1],
        match.start,
        match.end + match.suffix_len + match.law_len, ref_tag
    )
    ref_tag.contents = split_reference(
        ref_tag.string,
        match.end - match.start,
        match.suffix_len, soup)
    ref_tag.contents[-1]["type"] = match.law_match_type


def find_references_in_section(section, soup, extractor: StatutesExtractor, para, art):
    logs = []
    match = extractor.search(section.contents[-1])  # Search first match
    while match:
        if match.has_main_area():
            handle_reference_match(
                match, section, soup, para, art
            )
        match = extractor.search(
            section.contents[-1],
            pos=(0 if match.has_main_area() else match.end)
        )
    return logs


def find_references_in_soup(
    soup, extractor, para, art, text_tag_name="text"
):
    logs = []
    for text in soup.find_all(text_tag_name):
        if text.is_empty_element:
            continue
        assert text.string
        logs.extend(
            find_references_in_section(
                text, soup, extractor, para, art
            )
        )
    return logs


#######################################################
# Functions: references without preceding 'article' or §
#######################################################
#
#
#
# def pos_in_orig_string(i, stemmed, orig):
#     prefix = stemmed[:i]
#     stemmed_tokens = regex.findall(r"[\w']+|[\W']+", prefix)
#     orig_tokens = regex.findall(r"[\w']+|[\W']+", orig)
#     #     return len(''.join(orig_tokens[:len(stemmed_tokens)-1])) + len(stemmed_tokens[-1]) # Precise position
#     return len("".join(orig_tokens[: len(stemmed_tokens)]))  # Round to next boundary
#
#
# def law_keys_to_regex(keys, min_length, max_length=-1):
#     pattern = ""
#     for key in keys:
#         if len(key) >= min_length and (len(key) <= max_length or max_length == -1):
#             pattern += regex.escape(key) + r"|"
#     pattern = pattern[:-1]
#     full_pattern = r"\b(?>" + pattern + r")\b"
#     return regex.compile(full_pattern, flags=regex.IGNORECASE)
#
#
# def find_law_references_in_section(section, soup, law_regex_pattern, sanitizer):
#     for item in list(section.contents):
#         i_in_section = section.contents.index(item)
#         if type(item) is not bs4.element.NavigableString:
#             continue
#         test_string = sanitizer(item.string)
#         matches = law_regex_pattern.finditer(test_string)
#         for match in reversed(list(matches)):
#             orig_start = pos_in_orig_string(match.start(), test_string, item.string)
#             orig_end = pos_in_orig_string(match.end(), test_string, item.string)
#
#             ref_tag = soup.new_tag("reference", pattern="generic")
#
#             section.contents[i_in_section : i_in_section + 1] = add_tag(
#                 section.contents[i_in_section], orig_start, orig_end, ref_tag
#             )
