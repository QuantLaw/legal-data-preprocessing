# Roughly validate the input files
import os
import re

from common import ensure_exists
from statics import US_INPUT_PATH, US_ORIGINAL_PATH


def us_validate_input():
    """moves files into main dir and validate files roughly"""

    ensure_exists(US_ORIGINAL_PATH)

    subfolders = [f.name for f in os.scandir(US_INPUT_PATH) if f.is_dir()]
    for subfolder in subfolders:
        for item in os.listdir(f"{US_INPUT_PATH}/{subfolder}"):

            # Filter by filename pattern
            pattern = re.compile(r"(\d+)usc(\d+)(a)?\.html?", flags=re.IGNORECASE)
            match = pattern.fullmatch(item)
            if not match:
                continue

            new_name = f'{match[2]}{"1" if match[3] else "0"}_{match[1]}.htm'

            # Prevent overwriting files
            if os.path.exists(f"{US_ORIGINAL_PATH}/{new_name}"):
                print(f"{US_ORIGINAL_PATH}/{new_name} already exists")
            else:
                os.rename(
                    f"{US_INPUT_PATH}/{subfolder}/{item}",
                    f"{US_ORIGINAL_PATH}/{new_name}",
                )

    files = os.listdir(US_ORIGINAL_PATH)
    files = [f for f in files if f.endswith(".htm")]
    pattern = re.compile(r"(\d+)_(\d+)\.htm")
    years = {}
    for file in files:
        match = pattern.fullmatch(file)
        year = match[2]
        title = match[1]
        years[year] = years[year] if years.get(year) else []
        years[year].append(title)

    for idx in list(years.keys()):
        years[idx] = sorted(years[idx])

    print(f"{len(files)} files found")
    print(f"{len(years)} years found")

    for year in sorted(years.keys()):
        titles = years[year]
        print(f"{year}: n={len(titles)}, max='{max(titles)}'")
