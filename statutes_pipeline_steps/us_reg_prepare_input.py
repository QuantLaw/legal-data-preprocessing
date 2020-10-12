import os
import shutil

from statics import US_REG_INPUT_PATH, US_REG_ORIGINAL_PATH
from utils.common import ensure_exists


def us_reg_prepare_input():
    """moves files into main dir and validate files roughly"""

    ensure_exists(US_REG_ORIGINAL_PATH)

    year_subfolders = [f.name for f in os.scandir(US_REG_INPUT_PATH) if f.is_dir()]
    for year_subfolder in year_subfolders:
        title_subfolders = [
            f.name
            for f in os.scandir(os.path.join(US_REG_INPUT_PATH, year_subfolder))
            if f.is_dir()
        ]
        for title_subfolder in title_subfolders:
            for item in os.listdir(
                os.path.join(US_REG_INPUT_PATH, year_subfolder, title_subfolder)
            ):

                if not item.endswith(".xml"):
                    continue

                # Prevent overwriting files
                if os.path.exists(os.path.join(US_REG_ORIGINAL_PATH, item)):
                    print(f"{US_REG_ORIGINAL_PATH}/{item} already exists")
                else:
                    shutil.copy(
                        os.path.join(
                            US_REG_INPUT_PATH, year_subfolder, title_subfolder, item
                        ),
                        os.path.join(US_REG_ORIGINAL_PATH, item),
                    )
