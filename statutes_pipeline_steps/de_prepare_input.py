# Roughly validate the input files
import os
import shutil

from quantlaw.utils.files import ensure_exists

from statics import (
    DE_ORIGINAL_PATH,
    DE_REG_ORIGINAL_PATH,
    JURIS_EXPORT_GESETZE_LIST_PATH,
    JURIS_EXPORT_PATH,
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


def de_prepare_input(regulations):

    dest = DE_REG_ORIGINAL_PATH if regulations else DE_ORIGINAL_PATH

    with open(JURIS_EXPORT_GESETZE_LIST_PATH) as f:
        gesetze_dirs = f.read().strip().split("\n")
    copy_selected_doknrs(gesetze_dirs, dest)

    if regulations:
        with open(JURIS_EXPORT_RVO_LIST_PATH) as f:
            rvo_dirs = f.read().strip().split("\n")
        copy_selected_doknrs(rvo_dirs, DE_REG_ORIGINAL_PATH)
