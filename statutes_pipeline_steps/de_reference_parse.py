import itertools
import json
import os

from quantlaw.de_extract.statutes_parse import StatutesParser, StringCaseException
from quantlaw.de_extract.stemming import stem_law_name
from quantlaw.utils.beautiful_soup import create_soup, save_soup
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.pipeline import PipelineStep

from statics import (
    DE_HELPERS_PATH,
    DE_REFERENCE_AREAS_PATH,
    DE_REFERENCE_PARSED_LOG_PATH,
    DE_REFERENCE_PARSED_PATH,
    DE_RVO_HELPERS_PATH,
    DE_RVO_REFERENCE_AREAS_PATH,
    DE_RVO_REFERENCE_PARSED_LOG_PATH,
    DE_RVO_REFERENCE_PARSED_PATH,
)
from statutes_pipeline_steps.de_reference_parse_vso_list import (
    identify_reference_in_juris_vso_list,
)
from utils.common import (
    copy_xml_schema_to_data_folder,
    get_stemmed_law_names_for_filename,
)


class DeReferenceParseStep(PipelineStep):
    def __init__(self, law_names, *args, **kwargs):
        self.law_names = law_names
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite) -> list:
        src = DE_RVO_REFERENCE_AREAS_PATH if regulations else DE_REFERENCE_AREAS_PATH
        dest = DE_RVO_REFERENCE_PARSED_PATH if regulations else DE_REFERENCE_PARSED_PATH

        ensure_exists(dest)
        files = list_dir(src, ".xml")

        ensure_exists(dest)
        files = list_dir(src, ".xml")

        if not overwrite:
            existing_files = os.listdir(dest)
            files = list(filter(lambda f: f not in existing_files, files))

        copy_xml_schema_to_data_folder()

        return files

    def execute_item(self, item):
        src = DE_RVO_REFERENCE_AREAS_PATH if regulations else DE_REFERENCE_AREAS_PATH
        dest = DE_RVO_REFERENCE_PARSED_PATH if regulations else DE_REFERENCE_PARSED_PATH

        laws_lookup = get_stemmed_law_names_for_filename(item, self.law_names)
        parser = StatutesParser(laws_lookup)

        logs = list()

        # for debug
        logs.append(f"Start file - {item}")

        soup = create_soup(f"{src}/{item}")
        parse_reference_content_in_soup(soup, parser, debug_context=item)
        current_lawid = soup.document.attrs["key"].split("_")[1]
        identify_reference_law_name_in_soup(soup, parser, current_lawid)
        identify_lawreference_law_name_in_soup(soup, laws_lookup)

        identify_reference_in_juris_vso_list(soup, laws_lookup, laws_lookup_keys)

        save_soup(soup, f"{dest}/{item}")
        return logs

    def finish_execution(self, results):
        logs = list(itertools.chain.from_iterable(results))
        ensure_exists(DE_RVO_HELPERS_PATH if regulations else DE_HELPERS_PATH)
        with open(
                DE_RVO_REFERENCE_PARSED_LOG_PATH
                if regulations
                else DE_REFERENCE_PARSED_LOG_PATH,
                mode="w",
        ) as f:
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


def identify_reference_law_name_in_soup(soup, parser, current_lawid):
    for reference in soup.find_all("reference", {"pattern": "inline"}):

        lawid = parser.parse_law(
            reference.lawname.string, reference.lawname["type"], current_lawid
        )

        ref_parts = json.loads(reference["parsed_verbose"])

        if reference.lawname.attrs["type"] in ["internal", "dict", "sgb"]:
            for ref_part in ref_parts:
                if not lawid:
                    print(reference)
                ref_part.insert(0, ["Gesetz", lawid])
        reference["parsed_verbose"] = json.dumps(ref_parts, ensure_ascii=False)

        ref_parts = json.loads(reference["parsed"])
        if reference.lawname.attrs["type"] in ["internal", "dict", "sgb"]:
            for ref_part in ref_parts:
                assert lawid
                ref_part.insert(0, lawid)
        reference["parsed"] = json.dumps(ref_parts, ensure_ascii=False)


def identify_lawreference_law_name_in_soup(soup, laws_lookup):
    for reference in soup.find_all("reference", {"pattern": "generic"}):
        reference["parsed"] = [[laws_lookup[stem_law_name(reference.string)]]]
