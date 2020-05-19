import argparse

from de_decisions_pipeline_steps.a_download import download
from de_decisions_pipeline_steps.b_clean import clean
from de_decisions_pipeline_steps.c_hierarchy import hierarchy
from de_decisions_pipeline_steps.d_reference_areas_parse import reference_parse_areas
from de_decisions_pipeline_steps.e_network import network

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "steps", nargs="*", default=["all"], help="select a step to perform by name"
    )
    args = parser.parse_args()

    if args.steps == ["all"]:
        steps = ["download", "clean", "hierarchy", "references", "network"]
    else:
        steps = args.steps

    if "download" in steps:
        download()

    if "clean" in steps:
        clean()

    if "hierarchy" in steps:
        hierarchy()

    if "references" in steps:
        reference_parse_areas()

    if "network" in steps:
        network()
