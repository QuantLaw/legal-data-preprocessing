# Roughly validate the input files
import os
import shutil

from utils.common import ensure_exists
from statics import (
    JURIS_EXPORT_PATH,
    DE_ORIGINAL_PATH,
    JURIS_EXPORT_GESETZE_LIST_PATH,
    DE_RVO_ORIGINAL_PATH,
    JURIS_EXPORT_RVO_LIST_PATH,
)


def copy_selected_doknrs(selection_list, target_dir):
    ensure_exists(target_dir)
    for doknr in selection_list:
        version_filenames = [
            f for f in os.listdir(f"{JURIS_EXPORT_PATH}/{doknr}") if f.endswith(".xml")
        ]
        for version_filename in version_filenames:
            assert len(version_filename.split("_")) == 3
            shutil.copy(
                f"{JURIS_EXPORT_PATH}/{doknr}/{version_filename}",
                f"{target_dir}/{version_filename}",
            )


def de_prepare_input():

    with open(JURIS_EXPORT_GESETZE_LIST_PATH) as f:
        gesetze_dirs = f.read().strip().split("\n")
    copy_selected_doknrs(gesetze_dirs, DE_ORIGINAL_PATH)

    with open(JURIS_EXPORT_RVO_LIST_PATH) as f:
        rvo_dirs = f.read().strip().split("\n")
    copy_selected_doknrs(rvo_dirs, DE_RVO_ORIGINAL_PATH)
