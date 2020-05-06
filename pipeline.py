import argparse
import multiprocessing
import re
from multiprocessing.pool import Pool

import pandas as pd

from de_crossreference_edgelist import (
    de_crossreference_edgelist_prepare,
    de_crossreference_edgelist,
)
from de_crossreference_lookup import (
    de_crossreference_lookup_prepare,
    de_crossreference_lookup,
)
from de_validity_table import (
    de_validity_table_prepare,
    de_validity_table,
    de_validity_table_finish,
)
from common import get_stemmed_law_names_for_filename, str_to_bool, process_items
from de_law_names import de_law_names_finish, de_law_names, de_law_names_prepare
from de_reference_areas import (
    de_reference_areas_prepare,
    de_reference_areas_finish,
    de_reference_areas,
)
from de_reference_parse import (
    de_reference_parse_prepare,
    de_reference_parse,
    de_reference_parse_finish,
)
from de_to_xml import de_to_xml_prepare, de_to_xml
from de_validate_input import de_validate_input
from de_xml_headings import (
    de_xml_heading_order,
    de_xml_heading_order_prepare,
    de_xml_heading_order_finish,
)
from de_xml_nest import get_xml_heading_orders, de_xml_nest_prepare, de_xml_nest
from hierarchy_graph import hierarchy_graph, hierarchy_graph_prepare
from snapshot_mapping_edgelist import (
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
    DE_LAW_NAMES_PATH,
    DE_LAW_VALIDITIES_PATH,
)
from us_crossreference_edgelist import (
    us_crossreference_edgelist_prepare,
    us_crossreference_edgelist,
)
from crossreference_graph import crossreference_graph_prepare, crossreference_graph
from us_crossreference_lookup import (
    us_crossreference_lookup_prepare,
    us_crossreference_lookup,
)
from us_reference_areas import (
    us_reference_areas_prepare,
    us_reference_areas,
    us_reference_areas_finish,
)
from us_reference_parse import (
    us_reference_parse_prepare,
    us_reference_parse,
    us_reference_parse_finish,
)
from us_to_xml import us_to_xml, us_to_xml_prepare
from us_validate_input import us_validate_input


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
    args = parser.parse_args()

    steps = [step.lower() for step in args.steps]
    dataset = args.dataset.lower()
    use_multiprocessing = args.use_multiprocessing
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
            # "validate",
            "xml",
            "xml_headings",  # DE only
            "xml_nest",  # DE only
            "validity_table",  # DE only
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

    if "validate" in steps:
        if dataset == "us":
            us_validate_input()
        elif dataset == "de":
            de_validate_input()
        print("Validation: done")

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
            )
        print("Convert to xml: done")

    if "xml_headings" in steps:
        if dataset == "de":
            items = de_xml_heading_order_prepare(overwrite)
            orders = process_items(
                items,
                selected_items,
                action_method=de_xml_heading_order,
                use_multiprocessing=use_multiprocessing,
            )
            de_xml_heading_order_finish(orders)
            print("Get heading orders: done")

    if "xml_nest" in steps:
        if dataset == "de":
            items = de_xml_nest_prepare(overwrite)
            heading_orders = get_xml_heading_orders()
            process_items(
                items,
                selected_items,
                action_method=de_xml_nest,
                use_multiprocessing=use_multiprocessing,
                args=(heading_orders,),
            )
            print("Nest xml: done")

    if "validity_table" in steps:
        if dataset == "de":
            indices, xmls_dict = de_validity_table_prepare(overwrite)
            data = process_items(
                indices,
                [],  # Ignore filter
                action_method=de_validity_table,
                use_multiprocessing=use_multiprocessing,
                args=(xmls_dict,),
            )
            de_validity_table_finish(data, xmls_dict)

            print("Validity table: done")

    if "law_names" in steps:
        if dataset == "de":
            items, validity_table = de_law_names_prepare(overwrite)
            print("items", len(items))
            names = process_items(
                items,
                [],  # Ignore filter
                action_method=de_law_names,
                use_multiprocessing=use_multiprocessing,
                args=(validity_table,),
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
            df_law_names = pd.read_csv(DE_LAW_NAMES_PATH)
            df_validities = pd.read_csv(DE_LAW_VALIDITIES_PATH, index_col="filename")
            items = de_reference_areas_prepare(overwrite)
            logs = process_items(
                items,
                selected_items,
                action_method=de_reference_areas,
                use_multiprocessing=use_multiprocessing,
                args=(df_law_names, df_validities),
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
            df_law_names = pd.read_csv(DE_LAW_NAMES_PATH)
            df_validities = pd.read_csv(DE_LAW_VALIDITIES_PATH, index_col="filename")
            items = de_reference_parse_prepare(overwrite)
            logs = process_items(
                items,
                selected_items,
                action_method=de_reference_parse,
                use_multiprocessing=use_multiprocessing,
                args=(df_law_names, df_validities),
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

            items = de_crossreference_edgelist_prepare(overwrite, snapshots)
            process_items(
                items,
                [],
                action_method=de_crossreference_edgelist,
                use_multiprocessing=use_multiprocessing,
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
        elif dataset == "de":
            source_graph = f"{DE_CROSSREFERENCE_GRAPH_PATH}/subseqitems"
            source_text = DE_REFERENCE_PARSED_PATH
            destination = f"{DE_SNAPSHOT_MAPPING_EDGELIST_PATH}/subseqitems"

        items = snapshot_mapping_edgelist_prepare(
            overwrite, snapshots, source_graph, source_text, destination, interval
        )
        process_items(
            items,
            [],
            action_method=snapshot_mapping_edgelist,
            use_multiprocessing=use_multiprocessing,
            processes=2,
            args=(source_graph, source_text, destination),
        )
        print("Make snapshot mapping: done")
