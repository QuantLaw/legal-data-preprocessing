import itertools
import json
import os

import regex

from utils.common import (
    list_dir,
    ensure_exists,
    create_soup,
    save_soup,
)
from statics import (
    US_REFERENCE_PARSED_PATH,
    US_REFERENCE_AREAS_PATH,
    US_HELPERS_PATH,
    US_REFERENCE_PARSED_LOG_PATH,
)


def us_reference_parse_prepare(overwrite):
    ensure_exists(US_REFERENCE_PARSED_PATH)
    files = list_dir(US_REFERENCE_AREAS_PATH, ".xml")

    if not overwrite:
        existing_files = os.listdir(US_REFERENCE_PARSED_PATH)
        files = list(filter(lambda f: f not in existing_files, files))

    return files


def us_reference_parse(filename):
    soup = create_soup(f"{US_REFERENCE_AREAS_PATH}/{filename}")

    this_title = get_title_from_filename(filename)
    logs = parse_references(soup, this_title)
    save_soup(soup, f"{US_REFERENCE_PARSED_PATH}/{filename}")
    return logs


def us_reference_parse_finish(logs_per_file):
    logs = list(itertools.chain.from_iterable(logs_per_file))
    ensure_exists(US_HELPERS_PATH)
    with open(US_REFERENCE_PARSED_LOG_PATH, mode="w") as f:
        f.write("\n".join(sorted(logs, key=lambda x: x.lower())))


###########
# Functions
###########


def get_title_from_filename(filename):
    base = os.path.splitext(filename)[0]
    title_key = base.split("_")[0]
    assert title_key[-1] == "0"
    assert len(title_key) == 3
    return int(title_key[:-1])


def sortable_paragraph_number(string):
    MIN_DIGITS = 4
    digits = len(regex.match(r"^\d*", string)[0])
    if not digits:
        return string
    return "0 " * (MIN_DIGITS - digits) + string


split_pattern_short = regex.compile(r"\s*U\.?S\.?C\.?\s*", flags=regex.IGNORECASE)
split_pattern_inline = regex.compile(
    r"\s*of\s+(?=(?:this\s+)?title\s*)", flags=regex.IGNORECASE
)
sub_split_pattern = regex.compile(
    r"\s*,?\s*(?:and|or|,|;|throu?g?h?|to)\s+", flags=regex.IGNORECASE
)


def get_enum_types(string):
    return (
        bool(regex.fullmatch(r"[a-z]", string)),
        bool(regex.fullmatch(r"\d+", string)),
        bool(regex.fullmatch(r"[A-Z]", string)),
        bool(regex.fullmatch(r"[xvi]x{0,4}v?i{0,4}", string)),
        bool(regex.fullmatch(r"[XVI]X{0,4}V?I{0,4}", string)),
        bool(regex.fullmatch(r"([a-z])\1", string)),
    )


def enum_types_match(x, y):
    for a, b in zip(x, y):
        if a and b:
            return True
    return False


# fmt: off

inline_title_pattern = regex.compile(
    r'(this)\stitle|'
    r'title\s(\d+)',
    flags=regex.IGNORECASE
)

# fmt: on


def extract_title_inline(text, this_title):
    match = inline_title_pattern.fullmatch(text)
    assert match
    if match[1]:
        return this_title
    elif match[2]:
        return int(match[2])
    else:
        raise Exception(text)


def parse_references(soup, this_title):
    test_list = []  # For debug
    for ref_tag in soup.find_all("reference"):
        # Split into title and subtitle
        if ref_tag["pattern"] == "block":
            text_parts = split_pattern_short.split(ref_tag.string)
            if not len(text_parts) == 2:
                raise Exception(str(ref_tag))
            title = int(text_parts[0].strip())
            sub_text = text_parts[1]
        elif ref_tag["pattern"] == "inline":
            text_parts = split_pattern_inline.split(ref_tag.string)
            if not len(text_parts) == 2:
                raise Exception(str(ref_tag))
            title = extract_title_inline(text_parts[1].strip(), this_title)
            sub_text = text_parts[0]
        else:
            raise Exception(f"{str(ref_tag)} has not matching pattern")

        # Preformat ranges
        for match in regex.finditer(
            r"(\d+[a-z]{0,3})[\-\–\—\.](\d+[a-z]{0,3})",
            sub_text,
            flags=regex.IGNORECASE,
        ):
            if sortable_paragraph_number(match[1]) <= sortable_paragraph_number(
                match[2]
            ):
                sub_text = f"{sub_text[:match.start()]}{match[1]} through {match[2]}{sub_text[match.end():]}"

        sub_text = sub_text.replace(" and following", " et. seq.")

        references = []
        text_sub_splitted = sub_split_pattern.split(sub_text)
        for test_text in text_sub_splitted:
            match = regex.fullmatch(
                r"(?:§|sec\.|section\b)?\s*"
                r"(\d+[a-z]{0,3}(?:[\-\–\—\.]\d+[a-z]{0,3})?)?"
                r"\s?"
                r"((?:\((?:\d*[a-z]{0,3})\))*)"
                r"("
                r" et\.? seq\.?|"
                r" and following"
                r")?",
                test_text,
                flags=regex.IGNORECASE,
            )
            if not match:
                #                 test_list.append(f'{test_text} -- {sub_text} -- {file}')
                continue
            sections = [match[1]]
            sub_sections = regex.split(r"[\(\)]+", match[2])
            sub_sections = [o for o in sub_sections if len(o)]
            sections.extend(sub_sections)

            if sections[0]:
                references.append(sections)
            else:
                new_reference = None
                current_part_types = get_enum_types(sections[1])
                for old_part in reversed(references[-1][1:]):
                    if enum_types_match(current_part_types, get_enum_types(old_part)):
                        new_reference = references[-1][: references[-1].index(old_part)]
                        break
                if not new_reference:
                    new_reference = references[-1][:]
                new_reference.extend(sections[1:])
                references.append(new_reference)

        # Add title to index 0 of reference
        for reference in references:
            reference.insert(0, str(title))

        ref_tag["parsed"] = json.dumps(references, ensure_ascii=False)
        test_list.append(f"{sub_text} -- {json.dumps(references, ensure_ascii=False)}")
    return test_list
