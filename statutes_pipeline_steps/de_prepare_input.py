# Roughly validate the input files
import os
import shutil

from quantlaw.utils.files import ensure_exists

from statics import DE_ORIGINAL_PATH, JURIS_EXPORT_GESETZE_LIST_PATH, JURIS_EXPORT_PATH


def de_prepare_input():

    ensure_exists(DE_ORIGINAL_PATH)

    with open(JURIS_EXPORT_GESETZE_LIST_PATH) as f:
        dirs = f.read().strip().split("\n")

    for doknr in dirs:
        version_filenames = [
            f for f in os.listdir(f"{JURIS_EXPORT_PATH}/{doknr}") if f.endswith(".xml")
        ]
        for version_filename in version_filenames:
            assert len(version_filename.split("_")) == 3
            shutil.copy(
                f"{JURIS_EXPORT_PATH}/{doknr}/{version_filename}",
                f"{DE_ORIGINAL_PATH}/{version_filename}",
            )
