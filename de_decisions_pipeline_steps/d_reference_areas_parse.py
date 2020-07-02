import multiprocessing
import os
import sys
import traceback

from bs4 import BeautifulSoup

from statics import (
    DE_DECISIONS_HIERARCHY,
    DE_DECISIONS_REFERENCE_AREAS,
    DE_DECISIONS_REFERENCE_PARSED_XML,
)
from statutes_pipeline_steps.de_reference_areas import find_references_in_soup
from statutes_pipeline_steps.de_reference_parse import (
    parse_reference_content_in_soup,
    identify_reference_law_name_in_soup,
    identify_lawreference_law_name_in_soup,
)
from utils.common import (
    list_dir,
    ensure_exists,
    save_soup,
    stem_law_name,
    load_law_names_compiled,
    get_stemmed_law_names,
)


def get_lawnames_date(requested_date):
    requested_date = requested_date.replace("-", "")
    lookup_date = None
    for date in sorted(law_names):
        if date <= requested_date:
            lookup_date = date
        else:
            break
    if not lookup_date:
        raise Exception(f"No lawnames for {lookup_date} not found.")
    return lookup_date


def find_references(decision):
    try:
        logs = []
        areas_exists = os.path.exists(f"{DE_DECISIONS_REFERENCE_AREAS}/{decision}")
        parsed_exists = os.path.exists(
            f"{DE_DECISIONS_REFERENCE_PARSED_XML}/{decision}"
        )

        if not (areas_exists and parsed_exists):  # General preparation
            with open(f"{DE_DECISIONS_HIERARCHY}/{decision}", encoding="utf8") as f:
                file_content = f.read()
            file_content = file_content.replace(
                '<!DOCTYPE dokument SYSTEM "http://www.rechtsprechung-im-internet.de/dtd/v1/rii-dok.dtd">',
                "",
            )
            soup = BeautifulSoup(file_content, "lxml-xml")

            # Get Entscheidungsdatum
            date = get_lawnames_date(soup.document.attrs["datum"])

            # Get laws in effect at time of decision
            laws_lookup = get_stemmed_law_names(date, law_names)
            laws_lookup_keys = sorted(laws_lookup.keys(), reverse=True)

        if not areas_exists:
            logs.append(
                find_references_in_soup(
                    soup,
                    laws_lookup,
                    laws_lookup_keys,
                    para=0,
                    art=0,
                    text_tag_name=["text", "norm"],
                )
                # set para and atr to 0 that refernece with naming a law are ignored.
            )
            save_soup(soup, f"{DE_DECISIONS_REFERENCE_AREAS}/{decision}")

        if not parsed_exists:
            with open(
                f"{DE_DECISIONS_REFERENCE_AREAS}/{decision}", encoding="utf8"
            ) as f:
                soup = BeautifulSoup(f.read(), "lxml-xml")
            parse_reference_content_in_soup(soup, decision)
            identify_reference_law_name_in_soup(
                soup, laws_lookup, laws_lookup_keys, current_lawid=None
            )
            identify_lawreference_law_name_in_soup(soup, laws_lookup)

            save_soup(soup, f"{DE_DECISIONS_REFERENCE_PARSED_XML}/{decision}")
    except:
        print("-----", decision, "-----")
        the_type, the_value, the_traceback = sys.exc_info()
        traceback.print_exception(the_type, the_value, the_traceback)
        raise


def reference_parse_areas(regulations):
    global law_names
    law_names = load_law_names_compiled(regulations)
    ensure_exists(DE_DECISIONS_REFERENCE_AREAS)
    ensure_exists(DE_DECISIONS_REFERENCE_PARSED_XML)
    decisions = list_dir(DE_DECISIONS_HIERARCHY, ".xml")
    with multiprocessing.Pool() as p:
        p.map(find_references, decisions)


# # REgZ extractor
#
# pattern = re.compile(
#     r'(?:'
#         r'(?:B\s+)?'
#         r'(?:\d?/?\d+|I?(?:X|V|I)+I*a?)'
#         r'\s+'
#         r'(?:B\s+)?'
#     r')?'
#     r'('
#         r'[A-Za-z\-Ü]+'
#         r'(?:\s*\((?:pat|Brfg|B|R|VZ|P|VS|Vs|Ü)\))?'
#     r')'
#     r'\s*'
#     r'(?:\d+\s*(?:,|\-|und)\s*)?'
#     r'\d+\/\d+a?'
#     r'(?:\s+\(PKH\)|\s+\(E[PU]\b\)?|\s+\(?[BRSKFCAD][LRH]?\)?)?'
#     r'(?:\s+\-\s+Vz\s+\d+/\d+)?'
#     r'\)?'
#     r'(?:\s+\(vormals\s.+)?'
# )
# keys = list(azs)
# az_splitted = [
#     [
#         az
#         for az in re.split(
#         r'\s+hinzuverb\.,\s+|,?\s*\(?\bverb\.\s*mi?t?\b\.?,?\s*|(?:,?\s+und|,?\s+zu|,),?\s(?!\d+/\d)(?:hinzuverb\.\s+)?|\s\((?=(?:\d+|I?(?:X|V|I)+I*a?)+\s+[A-Z]+\s+\d+/\d+)|\szu(?=\d\s)',
#         azs[k])
#     ]
#     for k in keys
# ]
#
# regZ = [
#     [
#         pattern.fullmatch(az)
#         for az in az_list
#     ]
#     for az_list in az_splitted
# ]
# regZ = [
#     [
#         match and match.group(1)
#         for match in match_list
#     ]
#     for match_list in regZ
# ]
