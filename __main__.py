import argparse
import multiprocessing
import re

from statutes_pipeline_steps.de_crossreference_edgelist import (
    de_crossreference_edgelist_prepare,
    de_crossreference_edgelist,
)
from statutes_pipeline_steps.de_crossreference_lookup import (
    de_crossreference_lookup_prepare,
    de_crossreference_lookup,
)
from statutes_pipeline_steps.de_prepare_input import de_prepare_input

from utils.common import (
    str_to_bool,
    process_items,
    load_law_names_compiled,
    load_law_names,
)
from statutes_pipeline_steps.de_law_names import (
    de_law_names_finish,
    de_law_names,
    de_law_names_prepare,
)
from statutes_pipeline_steps.de_reference_areas import (
    de_reference_areas_prepare,
    de_reference_areas_finish,
    de_reference_areas,
)
from statutes_pipeline_steps.de_reference_parse import (
    de_reference_parse_prepare,
    de_reference_parse,
    de_reference_parse_finish,
)
from statutes_pipeline_steps.de_to_xml import de_to_xml_prepare, de_to_xml

from statutes_pipeline_steps.hierarchy_graph import (
    hierarchy_graph,
    hierarchy_graph_prepare,
)
from statutes_pipeline_steps.snapshot_mapping_edgelist import (
    snapshot_mapping_edgelist_prepare,
    snapshot_mapping_edgelist,
)
from statics import (
    US_REFERENCE_PARSED_PATH,
    US_HIERARCHY_GRAPH_PATH,
    US_CROSSREFERENCE_EDGELIST_PATH,
    US_CROSSREFERENCE_GRAPH_PATH,
    DE_HIERARCHY_GRAPH_PATH,
    DE_REFERENCE_PARSED_PATH,
    DE_CROSSREFERENCE_EDGELIST_PATH,
    DE_CROSSREFERENCE_GRAPH_PATH,
    US_SNAPSHOT_MAPPING_EDGELIST_PATH,
    DE_SNAPSHOT_MAPPING_EDGELIST_PATH,
    ALL_YEARS,
)
from statutes_pipeline_steps.us_crossreference_edgelist import (
    us_crossreference_edgelist_prepare,
    us_crossreference_edgelist,
)
from statutes_pipeline_steps.crossreference_graph import (
    crossreference_graph_prepare,
    crossreference_graph,
)
from statutes_pipeline_steps.us_crossreference_lookup import (
    us_crossreference_lookup_prepare,
    us_crossreference_lookup,
)
from statutes_pipeline_steps.us_reference_areas import (
    us_reference_areas_prepare,
    us_reference_areas,
    us_reference_areas_finish,
)
from statutes_pipeline_steps.us_reference_parse import (
    us_reference_parse_prepare,
    us_reference_parse,
    us_reference_parse_finish,
)
from statutes_pipeline_steps.us_to_xml import us_to_xml, us_to_xml_prepare
from statutes_pipeline_steps.us_prepare_input import us_prepare_input


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
            "snapshots for crossreferences. Eg. 2010-01-01 for de dataset or 2010 for us dataset. "
            "To run on whole research window: all"
        ),
    )
    parser.add_argument(
        "--interval",
        dest="interval",
        nargs="?",
        type=int,
        default=1,
        help=(
            "Only for snapshot_mapping_edgelist. Interval for mapped snapshots. Default 1 (snapshot)"
        ),
    )

    parser.add_argument(
        "-r",
        "--regulations",
        dest="regulations",
        action="store_const",
        const=True,
        default=False,
        help="Include regulations",
    )
    args = parser.parse_args()

    steps = [step.lower() for step in args.steps]
    dataset = args.dataset.lower()
    use_multiprocessing = args.use_multiprocessing
    overwrite = args.overwrite
    snapshots = args.snapshots
    interval = args.interval
    selected_items = args.filter or []
    regulations = args.regulations

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
            "snapshot_mapping_edgelist",  # creates edgelist to map nodes between snapshots for DYNAMIC graph
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
                        "E.g. for de --snapshots 2012-01-31 2013-01-31 or for us --snapshot 2001"
                    )

    if "prepare_input" in steps:
        if dataset == "us":
            us_prepare_input()
        elif dataset == "de":
            de_prepare_input(regulations)
        print("Filter input: done")

    if "xml" in steps:
        if dataset == "us":
            items = us_to_xml_prepare(overwrite)
            process_items(
                items,
                selected_items,
                action_method=us_to_xml,
                use_multiprocessing=use_multiprocessing,
                chunksize=10,
            )
        elif dataset == "de":
            items = de_to_xml_prepare(overwrite)
            process_items(
                items,
                selected_items,
                action_method=de_to_xml,
                use_multiprocessing=use_multiprocessing,
                args=[regulations],
            )
        print("Convert to xml: done")

    if "law_names" in steps:
        if dataset == "de":
            items = de_law_names_prepare(overwrite)
            names = process_items(
                items,
                [],  # Ignore filter
                action_method=de_law_names,
                use_multiprocessing=use_multiprocessing,
            )
            de_law_names_finish(names)

            print("Law names: done")

    if "reference_areas" in steps:
        if dataset == "us":
            items = us_reference_areas_prepare(overwrite)
            logs = process_items(
                items,
                selected_items,
                action_method=us_reference_areas,
                use_multiprocessing=use_multiprocessing,
                processes=int(multiprocessing.cpu_count() / 2),
            )
            us_reference_areas_finish(logs)

        elif dataset == "de":
            law_names = load_law_names_compiled()
            items = de_reference_areas_prepare(overwrite)
            logs = process_items(
                items,
                selected_items,
                action_method=de_reference_areas,
                use_multiprocessing=use_multiprocessing,
                args=(law_names,),
            )
            de_reference_areas_finish(logs)
        print("Extract reference areas: done")

    if "reference_parse" in steps:
        if dataset == "us":
            items = us_reference_parse_prepare(overwrite)
            logs = process_items(
                items,
                selected_items,
                action_method=us_reference_parse,
                use_multiprocessing=use_multiprocessing,
            )
            us_reference_parse_finish(logs)
        if dataset == "de":
            law_names = load_law_names_compiled()
            items = de_reference_parse_prepare(overwrite)
            logs = process_items(
                items,
                selected_items,
                action_method=de_reference_parse,
                use_multiprocessing=use_multiprocessing,
                args=(law_names,),
            )
            de_reference_parse_finish(logs)

            print("Parse references: done")

    if "hierarchy_graph" in steps:
        for subseqitems_conf in get_subseqitem_conf(args.subseqitems):
            if dataset == "us":
                source = US_REFERENCE_PARSED_PATH
                destination = f'{US_HIERARCHY_GRAPH_PATH}/{"subseqitems" if subseqitems_conf else "seqitems"}'
            elif dataset == "de":
                source = DE_REFERENCE_PARSED_PATH
                destination = f'{DE_HIERARCHY_GRAPH_PATH}/{"subseqitems" if subseqitems_conf else "seqitems"}'

            items = hierarchy_graph_prepare(overwrite, source, destination)
            process_items(
                items,
                selected_items,
                action_method=hierarchy_graph,
                use_multiprocessing=use_multiprocessing,
                args=(source, destination, subseqitems_conf),
            )
        print("Make hierarchy graphs: done")

    if "crossreference_lookup" in steps:
        if dataset == "us":
            items = us_crossreference_lookup_prepare(overwrite, snapshots)
            process_items(
                items,
                [],
                action_method=us_crossreference_lookup,
                use_multiprocessing=use_multiprocessing,
            )

        elif dataset == "de":
            items = de_crossreference_lookup_prepare(overwrite, snapshots)
            process_items(
                items,
                [],
                action_method=de_crossreference_lookup,
                use_multiprocessing=use_multiprocessing,
            )
        print("Create crossreference lookup: done")

    if "crossreference_edgelist" in steps:
        if dataset == "us":
            source = US_REFERENCE_PARSED_PATH
            destination = US_CROSSREFERENCE_EDGELIST_PATH

            items = us_crossreference_edgelist_prepare(overwrite, snapshots)
            process_items(
                items,
                [],
                action_method=us_crossreference_edgelist,
                use_multiprocessing=use_multiprocessing,
            )

        elif dataset == "de":  # TODO LATER unify US and DE py
            source = DE_REFERENCE_PARSED_PATH
            destination = DE_CROSSREFERENCE_EDGELIST_PATH

            law_names_data = load_law_names()
            items = de_crossreference_edgelist_prepare(overwrite, snapshots)
            process_items(
                items,
                [],
                action_method=de_crossreference_edgelist,
                use_multiprocessing=use_multiprocessing,
                args=(law_names_data,),
            )
        print("Create crossreference edgelist: done")

    if "crossreference_graph" in steps:
        for subseqitems_conf in get_subseqitem_conf(args.subseqitems):
            if dataset == "us":
                source = f'{US_HIERARCHY_GRAPH_PATH}/{"subseqitems" if subseqitems_conf else "seqitems"}'
                destination = f'{US_CROSSREFERENCE_GRAPH_PATH}/{"subseqitems" if subseqitems_conf else "seqitems"}'
                edgelist_folder = US_CROSSREFERENCE_EDGELIST_PATH
            elif dataset == "de":
                source = f'{DE_HIERARCHY_GRAPH_PATH}/{"subseqitems" if subseqitems_conf else "seqitems"}'
                destination = f'{DE_CROSSREFERENCE_GRAPH_PATH}/{"subseqitems" if subseqitems_conf else "seqitems"}'
                edgelist_folder = DE_CROSSREFERENCE_EDGELIST_PATH

            items = crossreference_graph_prepare(
                overwrite, snapshots, source, edgelist_folder, destination
            )
            process_items(
                items,
                [],
                action_method=crossreference_graph,
                use_multiprocessing=use_multiprocessing,
                args=(source, edgelist_folder, destination, subseqitems_conf),
            )
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

        items = snapshot_mapping_edgelist_prepare(
            overwrite, snapshots, source_graph, source_text, destination, interval
        )

        process_items(
            items,
            [],
            action_method=snapshot_mapping_edgelist,
            use_multiprocessing=use_multiprocessing,
            processes=2,
            args=(source_graph, source_text, destination, law_names_data),
        )
        print("Make snapshot mapping: done")
