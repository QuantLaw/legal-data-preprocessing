import itertools
import json
import multiprocessing
import os
from builtins import Exception

import regex
from quantlaw.utils.beautiful_soup import create_soup, save_soup
from quantlaw.utils.files import ensure_exists, list_dir

from statics import (
    US_HELPERS_PATH,
    US_REFERENCE_AREAS_PATH,
    US_REFERENCE_PARSED_LOG_PATH,
    US_REFERENCE_PARSED_PATH,
    US_REG_HELPERS_PATH,
    US_REG_REFERENCE_AREAS_PATH,
    US_REG_REFERENCE_PARSED_LOG_PATH,
    US_REG_REFERENCE_PARSED_PATH,
)
from utils.common import RegulationsPipelineStep


class UsReferenceParseStep(RegulationsPipelineStep):
    max_number_of_processes = max(int(multiprocessing.cpu_count() / 2), 1)

    def get_items(self, overwrite) -> list:
        src = (
            US_REG_REFERENCE_AREAS_PATH if self.regulations else US_REFERENCE_AREAS_PATH
        )
        dest = (
            US_REG_REFERENCE_PARSED_PATH
            if self.regulations
            else US_REFERENCE_PARSED_PATH
        )

        ensure_exists(dest)
        files = list_dir(src, ".xml")

        if not overwrite:
            existing_files = os.listdir(dest)
            files = list(filter(lambda f: f not in existing_files, files))
        return files

    def execute_item(self, item):
        from statutes_pipeline_steps.us_reference_reg import parse_authority_references

        src = (
            US_REG_REFERENCE_AREAS_PATH if self.regulations else US_REFERENCE_AREAS_PATH
        )
        dest = (
            US_REG_REFERENCE_PARSED_PATH
            if self.regulations
            else US_REFERENCE_PARSED_PATH
        )

        soup = create_soup(f"{src}/{item}")

        this_title = self.get_title_from_filename(item)
        try:
            logs = parse_references(soup, this_title, this_usc=not self.regulations)
            logs += parse_authority_references(soup)
        except Exception:
            print(item)
            raise
        save_soup(soup, f"{dest}/{item}")
        return logs

    def finish_execution(self, results):
        logs = list(itertools.chain.from_iterable(results))
        ensure_exists(US_REG_HELPERS_PATH if self.regulations else US_HELPERS_PATH)
        with open(
            US_REG_REFERENCE_PARSED_LOG_PATH
            if self.regulations
            else US_REFERENCE_PARSED_LOG_PATH,
            mode="w",
        ) as f:
            f.write("\n".join(sorted(logs, key=lambda x: x.lower())))

    def get_title_from_filename(self, filename):
        if self.regulations:
            base = os.path.splitext(filename)[0]
            assert base.startswith("cfr")
            title_key = base.split("_")[0][len("cfr") :]
            return int(title_key)
        else:
            base = os.path.splitext(filename)[0]
            title_key = base.split("_")[0]
            assert title_key[-1] == "0"
            assert len(title_key) == 3
            return int(title_key[:-1])


###########
# Functions
###########


def sortable_paragraph_number(string):
    MIN_DIGITS = 4
    digits = len(regex.match(r"^\d*", string)[0])
    if not digits:
        return string
    return "0 " * (MIN_DIGITS - digits) + string


split_pattern_short = regex.compile(
    r"\s*(?:\b|(?<=\d))(U\.?S\.?C|C\.?F\.?R)(?:\.|\b|(?=\d)|Sec\.)\s*",
    flags=regex.IGNORECASE,
)
split_pattern_inline = regex.compile(
    # fmt: off
    r"\s*of\s+(?=(?:"
        r'(?:this\s(?:sub\-?)?(?:title|chapter|part|section|division|paragraph))'
    r'|'
        r'(?:title)'
    r"))"
    # fmt: on
    ,
    flags=regex.IGNORECASE,
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
    r'(?:'
        r'(this)\s(?:sub\-?)?(?:title|chapter|part|section|division|paragraph)'
    r'|'
        r'title\s(\d+)'
    r')'
    r'(\s+of\s+the\s+Code\s+of\s+Federal\s+Regulations)?'
    r'(\s+of\s+the\s+Code\s+of\s+the\s+United\s+States)?',
    flags=regex.IGNORECASE
)

# fmt: on


def extract_title_inline(text, this_title, this_usc):
    match = inline_title_pattern.fullmatch(text)
    assert match

    if bool(match[4]):
        usc = True
    elif bool(match[3]):
        usc = False
    else:
        usc = this_usc

    if match[1]:
        return this_title, usc
    elif match[2]:
        return int(match[2]), usc
    else:
        raise Exception(text)


def split_block_reference(reference_str, debug_context=None):
    text_parts = split_pattern_short.split(reference_str)
    if not len(text_parts) == 3:
        print("ERROR", text_parts, str(debug_context))
    title = int(text_parts[0].strip())
    usc = "u" in text_parts[1].lower()
    sub_text = text_parts[2]
    return usc, title, sub_text


def parse_references(soup, this_title, this_usc):
    test_list = []  # For debug
    for ref_tag in soup.find_all("reference"):
        # Split into title and subtitle
        last_usc = None
        last_title = None
        if ref_tag["pattern"] == "block":
            usc, title, sub_text = split_block_reference(
                ref_tag.string, debug_context=ref_tag
            )
            text_parts = split_pattern_inline.split(sub_text)
            if len(text_parts) == 2:
                last_title, last_usc = extract_title_inline(
                    text_parts[1].strip(), this_title, this_usc
                )
                sub_text = text_parts[0]
            elif len(text_parts) > 2:
                raise Exception(str(ref_tag))

        elif ref_tag["pattern"] == "inline":
            text_parts = split_pattern_inline.split(ref_tag.string)
            if len(text_parts) == 2:
                title, usc = extract_title_inline(
                    text_parts[1].strip(), this_title, this_usc
                )
                sub_text = text_parts[0]
            elif len(text_parts) == 1:
                title = this_title
                sub_text = text_parts[0].strip()
                usc = this_usc
            else:
                raise Exception(str(ref_tag))
        else:
            raise Exception(f"{str(ref_tag)} has not matching pattern")

        references = parse_reference_text(sub_text)
        add_title_to_reference(references, title, usc, last_title, last_usc)

        ref_tag["parsed"] = json.dumps(references, ensure_ascii=False)
        test_list.append(f"{sub_text} -- {json.dumps(references, ensure_ascii=False)}")
    return test_list


def add_title_to_reference(references, title, usc, last_title=None, last_usc=None):
    # Add title to index 0 of reference
    for reference in references:
        if usc:
            title_str = str(title)
        else:
            title_str = "cfr" + str(title)
        reference.insert(0, title_str)
    if len(references) > 1 and last_title is not None:
        assert last_usc is not None
        if last_usc:
            title_str = str(last_title)
        else:
            title_str = "cfr" + str(last_title)
        references[-1][0] = title_str


def parse_reference_text(sub_text):
    # Preformat ranges
    for match in regex.finditer(
        r"(\d+[a-z]{0,3})[\-\–\—](\d+[a-z]{0,3})",
        sub_text,
        flags=regex.IGNORECASE,
    ):
        if sortable_paragraph_number(match[1]) <= sortable_paragraph_number(match[2]):
            sub_text = (
                f"{sub_text[:match.start()]}{match[1]} through "
                f"{match[2]}{sub_text[match.end():]}"
            )

    sub_text = sub_text.replace(" and following", " et. seq.").strip()

    references = []
    text_sub_splitted = sub_split_pattern.split(sub_text)
    for test_text in text_sub_splitted:
        match = regex.fullmatch(
            r"(?:§+|sec\.|sections?\b|(?:sub)?parts?\b)?\s*"
            r"(\d+[a-z]{0,3}(?:[\-\–\—\.]\d+[a-z]{0,3})?)"
            r"\s?"
            r"((?:\((?:\d*[a-z]{0,4})\))*)"
            r"("
            r" et\.? seq\.?|"
            r" and following"
            r")?",
            test_text,
            flags=regex.IGNORECASE,
        )
        if not match:
            # test_list.append(f'{test_text} -- {sub_text} -- {file}')
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
    return references
