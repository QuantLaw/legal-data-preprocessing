import glob
import os
import re
import shutil
from zipfile import ZipFile

import pandas as pd

from statics import US_REG_INPUT_COPY_LOG_PATH, US_REG_INPUT_PATH, US_REG_ORIGINAL_PATH
from utils.common import ensure_exists

pattern = re.compile(r".+/CFR-(?P<y>\d+)-title(?P<t>\d+)-vol(?P<v>\d*).xml")


def us_reg_prepare_input():
    """moves files into main dir and validate files roughly"""

    ensure_exists(US_REG_ORIGINAL_PATH)

    year_zips = sorted(
        [f.name for f in os.scandir(US_REG_INPUT_PATH) if f.name.endswith(".zip")]
    )
    for year_zip in year_zips:
        year = os.path.splitext(year_zip)[0]
        year_folder = os.path.join(US_REG_ORIGINAL_PATH, year)
        if os.path.exists(year_folder):
            raise Exception(f"{year_folder} already exists")

        with ZipFile(os.path.join(US_REG_INPUT_PATH, year_zip), "r") as zipObj:
            # Extract all the contents of zip file in current directory
            zipObj.extractall(year_folder)

    # Get all files
    vols = [
        pattern.fullmatch(p).groupdict()
        for p in glob.glob(os.path.join(US_REG_ORIGINAL_PATH, "*/*/*.xml"))
    ]

    print("Dropping")
    for vol in vols:
        if not vol["v"]:
            print(vol)
            os.remove(
                os.path.join(
                    US_REG_ORIGINAL_PATH,
                    vol["y"],
                    f"title-{vol['t']}",
                    f"CFR-{vol['y']}-title{vol['t']}-vol.xml",
                )
            )
    vols = [v for v in vols if v["v"]]

    df = pd.DataFrame(vols)
    df.v = [int(v) if v else None for v in df.v]
    df.y = [int(y) for y in df.y]
    df.t = [int(t) for t in df.t]
    df = df.sort_values(["y", "t", "v"]).reset_index().drop("index", axis=1)

    volumes = sorted({(t, v) for t, v in zip(df.t, df.v)})

    copy_actions = []

    for title, volume in volumes:
        vol_df = df[(df.t == title) & (df.v == volume)]
        existing_years = set(vol_df.y)
        last_exisiting_year = None
        for year in range(vol_df.y.min(), vol_df.y.max()):
            if year in existing_years:
                last_exisiting_year = year
            else:
                assert last_exisiting_year
                copy_actions.append(
                    dict(
                        title=title,
                        volume=volume,
                        from_year=last_exisiting_year,
                        to_year=year,
                    )
                )
    for copy_action in copy_actions:
        to_dir = os.path.join(
            US_REG_ORIGINAL_PATH,
            str(copy_action["to_year"]),
            f"title-{copy_action['title']}",
        )
        os.makedirs(to_dir, exist_ok=True)
        shutil.copy(
            os.path.join(
                US_REG_ORIGINAL_PATH,
                str(copy_action["from_year"]),
                f"title-{copy_action['title']}",
                f"CFR-{copy_action['from_year']}-"
                f"title{copy_action['title']}-"
                f"vol{copy_action['volume']}.xml",
            ),
            os.path.join(
                to_dir,
                f"CFR-{copy_action['to_year']}-"
                f"title{copy_action['title']}-"
                f"vol{copy_action['volume']}.xml",
            ),
        )
    pd.DataFrame(copy_actions).to_csv(US_REG_INPUT_COPY_LOG_PATH, index=False)
