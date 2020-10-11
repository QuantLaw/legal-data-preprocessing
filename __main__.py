import argparse
import os
import re

from statics import (
    ALL_YEARS,
    DE_CROSSREFERENCE_EDGELIST_PATH,
    DE_CROSSREFERENCE_GRAPH_PATH,
    DE_HIERARCHY_GRAPH_PATH,
    DE_REFERENCE_PARSED_PATH,
    DE_SNAPSHOT_MAPPING_EDGELIST_PATH,
    US_CROSSREFERENCE_EDGELIST_PATH,
    US_CROSSREFERENCE_GRAPH_PATH,
    US_HIERARCHY_GRAPH_PATH,
    US_REFERENCE_PARSED_PATH,
    US_SNAPSHOT_MAPPING_EDGELIST_PATH,
)
from statutes_pipeline_steps.crossreference_graph import CrossreferenceGraphStep
from statutes_pipeline_steps.de_crossreference_edgelist import DeCrossreferenceEdgelist
from statutes_pipeline_steps.de_crossreference_lookup import DeCrossreferenceLookup
from statutes_pipeline_steps.de_law_names import DeLawNamesStep
from statutes_pipeline_steps.de_prepare_input import de_prepare_input
from statutes_pipeline_steps.de_reference_areas import DeReferenceAreasStep
from statutes_pipeline_steps.de_reference_parse import DeReferenceParseStep
from statutes_pipeline_steps.de_to_xml import DeToXmlStep
from statutes_pipeline_steps.hierarchy_graph import HierarchyGraphStep
from statutes_pipeline_steps.snapshot_mapping_edgelist import (
    SnapshotMappingEdgelistStep,
)
from statutes_pipeline_steps.us_crossreference_edgelist import UsCrossreferenceEdgelist
from statutes_pipeline_steps.us_crossreference_lookup import UsCrossreferenceLookup
from statutes_pipeline_steps.us_prepare_input import us_prepare_input
from statutes_pipeline_steps.us_reference_areas import UsReferenceAreasStep
from statutes_pipeline_steps.us_reference_parse import UsReferenceParseStep
from statutes_pipeline_steps.us_to_xml import UsToXmlStep
from utils.common import load_law_names, load_law_names_compiled, str_to_bool


def get_subseqitem_conf(subseqitems):
    if subseqitems is None:
        return False, True
    elif subseqitems is True:
        return (True,)
    elif subseqitems is False:
        return (False,)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", help="select a dataset: DE or US")
    parser.add_argument("steps", nargs="+", help="select a step to perform by name")
    parser.add_argument("--filter", nargs="*", help="filter for specific files")
    parser.add_argument(
        "--single-process",
        dest="use_multiprocessing",
        action="store_const",
        const=False,
        default=True,
        help="prevent multiprocessing",
    )
    parser.add_argument(
        "--overwrite",
        dest="overwrite",
        action="store_const",
        const=True,
        default=False,
        help="overwrite files",
    )
    parser.add_argument(
        "--subseqitems",
        dest="subseqitems",
        nargs="?",
        const=True,
        type=str_to_bool,
        default=None,
        help="include subseqitems in graphs",
    )
    parser.add_argument(
        "--snapshots",
        dest="snapshots",
        nargs="*",
        type=str,
        default=["all"],
        help=(
            "snapshots for crossreferences. Eg. 2010-01-01 for de dataset or 2010 for "
            "us dataset. To run on whole research window: all"
        ),
    )
    parser.add_argument(
        "--interval",
        dest="interval",
        nargs="?",
        type=int,
        default=1,
        help=(
            "Only for snapshot_mapping_edgelist. Interval for mapped snapshots. "
            "Default 1 (snapshot)"
        ),
    )
    args = parser.parse_args()

    steps = [step.lower() for step in args.steps]
    dataset = args.dataset.lower()
    use_multiprocessing = args.use_multiprocessing
    processes = None if args.use_multiprocessing else 1
    overwrite = args.overwrite
    snapshots = args.snapshots
    interval = args.interval
    selected_items = args.filter or []

    if dataset not in ["de", "us"]:
        raise Exception(f"{dataset} unsupported dataset. Options: us, de")

    if "all" in snapshots:
        if dataset == "us":
            snapshots = [f"{year}" for year in ALL_YEARS]
        elif dataset == "de":
            snapshots = [f"{year}-01-01" for year in ALL_YEARS]

    if "all" in steps:
        steps = [
            "prepare_input",
            "xml",
            "law_names",  # DE only
            "reference_areas",
            "reference_parse",
            "hierarchy_graph",
            "crossreference_lookup",
            "crossreference_edgelist",
            "crossreference_graph",
            # creates edgelist to map nodes between snapshots for DYNAMIC graph
            "snapshot_mapping_edgelist",
        ]

    if (
        "crossreference_lookup" in steps
        or "crossreference_edgelist" in steps
        or "crossreference_graph" in steps
    ):
        if dataset == "de" or snapshots:
            for snapshot in snapshots:
                if not re.fullmatch(r"\d{4}(-\d{2}-\d{2})?", snapshot):
                    raise Exception(
                        "Add --snapshots as argument. "
                        "E.g. for de --snapshots 2012-01-31 2013-01-31 or for us "
                        "--snapshot 2001"
                    )

    if "prepare_input" in steps:
        if dataset == "us":
            us_prepare_input()
        elif dataset == "de":
            de_prepare_input()
        print("Filter input: done")

    if "xml" in steps:
        if dataset == "us":
            step = UsToXmlStep(processes)
            items = step.get_items(overwrite)
            step.execute_filtered_items(items, selected_items)
        elif dataset == "de":
            step = DeToXmlStep(processes)
            items = step.get_items(overwrite)
            step.execute_filtered_items(items, selected_items)
        print("Convert to xml: done")

    if "law_names" in steps:
        if dataset == "de":
            step = DeLawNamesStep(processes)
            items = step.get_items()
            step.execute_items(items)
            print("Law names: done")

    if "reference_areas" in steps:
        if dataset == "us":
            step = UsReferenceAreasStep(processes)
            items = step.get_items(overwrite)
            step.execute_filtered_items(items)

        elif dataset == "de":
            law_names = load_law_names_compiled()
            step = DeReferenceAreasStep(law_names, processes)
            items = step.get_items(overwrite)
            step.execute_filtered_items(items)

        print("Extract reference areas: done")

    if "reference_parse" in steps:
        if dataset == "us":
            step = UsReferenceParseStep(processes)
            items = step.get_items(overwrite)
            step.execute_filtered_items(items)
        if dataset == "de":
            law_names = load_law_names_compiled()
            step = DeReferenceParseStep(law_names, processes)
            items = step.get_items(overwrite)
            step.execute_filtered_items(items)

            print("Parse references: done")

    if "hierarchy_graph" in steps:
        for subseqitems_conf in get_subseqitem_conf(args.subseqitems):
            if dataset == "us":
                source = US_REFERENCE_PARSED_PATH
                destination = os.path.join(
                    US_HIERARCHY_GRAPH_PATH,
                    "subseqitems" if subseqitems_conf else "seqitems",
                )
            elif dataset == "de":
                source = DE_REFERENCE_PARSED_PATH
                destination = os.path.join(
                    DE_HIERARCHY_GRAPH_PATH,
                    "subseqitems" if subseqitems_conf else "seqitems",
                )

            step = HierarchyGraphStep(
                source=source,
                destination=destination,
                add_subseqitems=subseqitems_conf,
                processes=processes,
            )
            items = step.get_items(overwrite)
            step.execute_filtered_items(items)
        print("Make hierarchy graphs: done")

    if "crossreference_lookup" in steps:
        if dataset == "us":
            step = UsCrossreferenceLookup(processes)
            items = step.get_items(overwrite, snapshots)
            step.execute_items(items)

        elif dataset == "de":
            step = DeCrossreferenceLookup(processes)
            items = step.get_items(snapshots)
            step.execute_items(items)

        print("Create crossreference lookup: done")

    if "crossreference_edgelist" in steps:
        if dataset == "us":
            step = UsCrossreferenceEdgelist(processes)
            items = step.get_items(overwrite, snapshots)
            step.execute_items(items)

        elif dataset == "de":
            law_names_data = load_law_names()
            step = DeCrossreferenceEdgelist(law_names_data, processes)
            items = step.get_items(overwrite, snapshots)
            step.execute_items(items)

        print("Create crossreference edgelist: done")

    if "crossreference_graph" in steps:
        for subseqitems_conf in get_subseqitem_conf(args.subseqitems):
            if dataset == "us":
                source = os.path.join(
                    US_HIERARCHY_GRAPH_PATH,
                    "subseqitems" if subseqitems_conf else "seqitems",
                )
                destination = os.path.join(
                    US_CROSSREFERENCE_GRAPH_PATH,
                    "subseqitems" if subseqitems_conf else "seqitems",
                )
                edgelist_folder = US_CROSSREFERENCE_EDGELIST_PATH
            elif dataset == "de":
                source = os.path.join(
                    DE_HIERARCHY_GRAPH_PATH,
                    "subseqitems" if subseqitems_conf else "seqitems",
                )
                destination = os.path.join(
                    DE_CROSSREFERENCE_GRAPH_PATH,
                    "subseqitems" if subseqitems_conf else "seqitems",
                )
                edgelist_folder = DE_CROSSREFERENCE_EDGELIST_PATH

            step = CrossreferenceGraphStep(
                source=source,
                destination=destination,
                edgelist_folder=edgelist_folder,
                dataset=dataset,
                processes=processes,
            )
            items = step.get_items(overwrite, snapshots)
            step.execute_items(items)

        print("Make crossreference graph: done")

    if "snapshot_mapping_edgelist" in steps:
        if dataset == "us":
            source_graph = f"{US_CROSSREFERENCE_GRAPH_PATH}/subseqitems"
            source_text = US_REFERENCE_PARSED_PATH
            destination = f"{US_SNAPSHOT_MAPPING_EDGELIST_PATH}/subseqitems"
            law_names_data = None
        elif dataset == "de":
            source_graph = f"{DE_CROSSREFERENCE_GRAPH_PATH}/subseqitems"
            source_text = DE_REFERENCE_PARSED_PATH
            destination = f"{DE_SNAPSHOT_MAPPING_EDGELIST_PATH}/subseqitems"
            law_names_data = load_law_names()

        step = SnapshotMappingEdgelistStep(
            source_graph, source_text, destination, interval, dataset, law_names_data
        )
        items = step.get_items(overwrite, snapshots)
        step.execute_items(items)

        print("Make snapshot mapping: done")
