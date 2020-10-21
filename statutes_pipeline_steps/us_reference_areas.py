import itertools
import multiprocessing
import os

import bs4
from quantlaw.utils.beautiful_soup import create_soup, save_soup
from quantlaw.utils.files import ensure_exists, list_dir
from regex import regex

from statics import (
    US_HELPERS_PATH,
    US_REFERENCE_AREAS_LOG_PATH,
    US_REFERENCE_AREAS_PATH,
    US_REG_HELPERS_PATH,
    US_REG_REFERENCE_AREAS_LOG_PATH,
    US_REG_REFERENCE_AREAS_PATH,
    US_REG_XML_PATH,
    US_XML_PATH,
)
from utils.common import RegulationsPipelineStep


class UsReferenceAreasStep(RegulationsPipelineStep):
    max_number_of_processes = max(int(multiprocessing.cpu_count() / 2), 1)

    def get_items(self, overwrite) -> list:
        src = US_REG_XML_PATH if self.regulations else US_XML_PATH
        dest = (
            US_REG_REFERENCE_AREAS_PATH if self.regulations else US_REFERENCE_AREAS_PATH
        )
        ensure_exists(dest)
        files = list_dir(src, ".xml")

        if not overwrite:
            existing_files = os.listdir(dest)
            files = list(filter(lambda f: f not in existing_files, files))

        return files

    def execute_item(self, item):
        src = US_REG_XML_PATH if self.regulations else US_XML_PATH
        dest = (
            US_REG_REFERENCE_AREAS_PATH if self.regulations else US_REFERENCE_AREAS_PATH
        )
        soup = create_soup(f"{src}/{item}")
        logs = find_references(soup, usc_pattern, {"pattern": "block"})
        logs += find_references(soup, inline_pattern, {"pattern": "inline"})
        save_soup(soup, f"{dest}/{item}")
        return logs

    def finish_execution(self, results):
        logs = list(itertools.chain.from_iterable(results))
        ensure_exists(US_REG_HELPERS_PATH if self.regulations else US_HELPERS_PATH)
        log_path = (
            US_REG_REFERENCE_AREAS_LOG_PATH
            if self.regulations
            else US_REFERENCE_AREAS_LOG_PATH
        )
        with open(log_path, mode="w") as f:
            f.write("\n".join(sorted(logs, key=lambda x: x.lower())))


################
# Regex patterns
################

# fmt: off

regex_definitions = (
    r'(?(DEFINE)'
        r'(?<sec>'
            r'(\d+([\da-zA-Z\-\–\—\.]*[\da-zA-Z\-\–\—])?)'
            r'(\(\d*[a-z]{0,3}i*\))*'
            r'(\s+et\.?\s+seq\.?)?'
            r'(\s+and\sfollowing)?'
        r')'
        r'(?<numb>'
            r'(\(\d*[a-z]{0,2}i?\))+'
            r'(\s+et\.?\s+seq\.?)?'
        r')'
        r'(?<conn>'
            r',?\s+(and|or|to|through)(\sin)?\s+|'
            r'(,|;)\s+'
        r')'
    r')'
)

usc_pattern_string = regex_definitions + (
    r'('
        r'(\d+)\s*'
        r'('
            r'U\.?S\.?C\.?'
        r'|'
            r'C\.?F\.?R\.?'
        r')\s*'
        r'(Sec(?:tions?|\.)?|§§?|\b(sub)?Parts?)?\s*'
        r'(?&sec)'
        r'((?&conn)(Sec(?:tions|\.)?|§§?|\b(sub)?Parts?)?\s*(?&sec)|(?&conn)(?&numb))*'
    r')'
    r'(?!\w*(\sApp\.)?\s(U\.?S\.?C\.?|C\.?F\.?R\.?|Stat\.))'
)
usc_pattern = regex.compile(usc_pattern_string, flags=regex.IGNORECASE)

inline_pattern_string = regex_definitions + (
    r'(Sec(?:tion|\.)?|§§?|\b(sub)?parts?)\s*'
    r'(?&sec)'
    r'('
        r'(?&conn)'
        r'(Sec(?:tions?|\.)?|§§?)?'
        r'\s*'
        r'(?&sec)'
    r'|'
        r'(?&conn)(?&numb)'
    r')*'
    r'\s*'
    r'('
        r'(of\sthis\s(title|chapter|(sub)?part))'
    r'|'
        r'(of\stitle\s\d+)'
    r')?'
    r'(\s+of\s+the\s+Code\s+of\s+Federal\s+Regulations)?'
)
inline_pattern = regex.compile(inline_pattern_string, flags=regex.IGNORECASE)

# fmt: on

###########
# Functions
###########


def add_tag(string, pos, end, tag):
    """
    Wraps part of a string a given tag
    """
    tag.string = string[pos:end]
    return [
        bs4.element.NavigableString(string[:pos]),
        tag,
        bs4.element.NavigableString(string[end:]),
    ]


def find_references(soup, pattern, attrs):
    """
    Finds the references in the soup and marks them a tag
    """
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
                if regex.match(r"\s?,?of\b", text_tag_string[match.end() :]):
                    continue
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
