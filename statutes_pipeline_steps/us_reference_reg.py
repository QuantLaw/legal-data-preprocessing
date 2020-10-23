import json

from bs4 import BeautifulSoup
from regex.regex import Match

from statutes_pipeline_steps.us_reference_parse import (
    add_title_to_reference,
    parse_reference_text,
    split_block_reference,
)


def find_authority_references(soup: BeautifulSoup, pattern: Match):
    logs = []

    for tag in soup.find_all(auth_text=True):
        auth_text = tag.attrs["auth_text"]
        matches = [m[0] for m in pattern.finditer(auth_text)]
        tag.attrs["auth_text_areas"] = json.dumps(matches, ensure_ascii=False)
    return logs


def parse_authority_references(soup: BeautifulSoup):
    logs = []
    for tag in soup.find_all(auth_text_areas=True):
        auth_areas = json.loads(tag.attrs["auth_text_areas"])
        auth_parsed = []
        for auth_area in auth_areas:
            usc, title, sub_text = split_block_reference(
                auth_area, debug_context=tag.attrs["auth_text"]
            )
            references = parse_reference_text(sub_text)
            add_title_to_reference(references, title, usc)
            auth_parsed.append(references)
        tag.attrs["auth_text_parsed"] = json.dumps(auth_parsed, ensure_ascii=False)
    return logs
