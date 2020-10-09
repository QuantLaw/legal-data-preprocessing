import itertools
import json
import os

from quantlaw.de_extract.statutes_parse import StringCaseException, StatutesParser
from quantlaw.de_extract.stemming import stem_law_name
from quantlaw.utils.beautiful_soup import create_soup, save_soup
from quantlaw.utils.files import ensure_exists, list_dir

from utils.common import (
    get_stemmed_law_names_for_filename,
    copy_xml_schema_to_data_folder,
)
from statics import (
    DE_REFERENCE_AREAS_PATH,
    DE_HELPERS_PATH,
    DE_REFERENCE_PARSED_LOG_PATH,
    DE_REFERENCE_PARSED_PATH,
)


def de_reference_parse_prepare(overwrite):
    ensure_exists(DE_REFERENCE_PARSED_PATH)
    files = list_dir(DE_REFERENCE_AREAS_PATH, ".xml")

    if not overwrite:
        existing_files = os.listdir(DE_REFERENCE_PARSED_PATH)
        files = list(filter(lambda f: f not in existing_files, files))

    copy_xml_schema_to_data_folder()

    return files


def de_reference_parse(filename, law_names):
    laws_lookup = get_stemmed_law_names_for_filename(filename, law_names)
    parser = StatutesParser(laws_lookup)

    logs = list()

    # for debug
    logs.append(f"Start file - {filename}")

    soup = create_soup(f"{DE_REFERENCE_AREAS_PATH}/{filename}")
    parse_reference_content_in_soup(soup, parser, debug_context=filename)
    current_lawid = soup.document.attrs["key"].split("_")[1]
    identify_reference_law_name_in_soup(
        soup, parser, current_lawid
    )
    identify_lawreference_law_name_in_soup(soup, laws_lookup)

    save_soup(soup, f"{DE_REFERENCE_PARSED_PATH}/{filename}")
    return logs


def de_reference_parse_finish(logs_per_file):
    logs = list(itertools.chain.from_iterable(logs_per_file))
    ensure_exists(DE_HELPERS_PATH)
    with open(DE_REFERENCE_PARSED_LOG_PATH, mode="w") as f:
        f.write("\n".join(sorted(logs, key=lambda x: x.lower())))



def parse_reference_content(reference, parser):
    citation = reference.main.get_text()
    reference_paths = parser.parse_main(citation)

    reference["parsed_verbose"] = json.dumps(reference_paths, ensure_ascii=False)
    reference_paths_simple = [
        [component[1] for component in path] for path in reference_paths
    ]
    reference["parsed"] = json.dumps(reference_paths_simple, ensure_ascii=False)


def parse_reference_content_in_soup(soup, parser, debug_context=None):
    for reference in soup.find_all("reference", {"pattern": "inline"}):
        if reference.main:
            try:
                parse_reference_content(reference, parser)
            except StringCaseException as error:
                print(error, "context", debug_context)


def identify_reference_law_name_in_soup(
    soup, parser, current_lawid
):
    for reference in soup.find_all("reference", {"pattern": "inline"}):

        lawid = parser.parse_law(reference.lawname.string,reference.lawname["type"], current_lawid)

        ref_parts = json.loads(reference["parsed_verbose"])
        for ref_part in ref_parts:
            ref_part.insert(0, ["Gesetz", lawid])
        reference["parsed_verbose"] = json.dumps(ref_parts, ensure_ascii=False)

        ref_parts = json.loads(reference["parsed"])
        for ref_part in ref_parts:
            ref_part.insert(0, lawid)
        reference["parsed"] = json.dumps(ref_parts, ensure_ascii=False)


def identify_lawreference_law_name_in_soup(soup, laws_lookup):
    for reference in soup.find_all("reference", {"pattern": "generic"}):
        reference["parsed"] = [[laws_lookup[stem_law_name(reference.string)]]]
