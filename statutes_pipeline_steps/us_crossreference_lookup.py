import os

import lxml.etree
import pandas as pd
from quantlaw.utils.files import ensure_exists, list_dir

from statics import (
    US_CROSSREFERENCE_LOOKUP_PATH,
    US_REFERENCE_PARSED_PATH,
    US_REG_CROSSREFERENCE_LOOKUP_PATH,
    US_REG_REFERENCE_PARSED_PATH,
)
from utils.common import RegulationsPipelineStep


class UsCrossreferenceLookup(RegulationsPipelineStep):
    def __init__(self, detailed_crossreferences, *args, **kwargs):
        self.detailed_crossreferences = detailed_crossreferences
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(self.dest)

        # If snapshots not set, create list of all years
        if not snapshots:
            snapshots = sorted(
                set(
                    [
                        x.split(".")[0].split("_")[-1]
                        for x in list_dir(US_REFERENCE_PARSED_PATH, ".xml")
                    ]
                )
            )

        if not overwrite:
            existing_files = os.listdir(self.dest)
            snapshots = list(
                filter(lambda f: get_filename(f) not in existing_files, snapshots)
            )

        return snapshots

    @property
    def dest(self):
        return (
            US_REG_CROSSREFERENCE_LOOKUP_PATH
            if self.regulations
            else US_CROSSREFERENCE_LOOKUP_PATH
        ) + ("/detailed" if self.detailed_crossreferences else "")

    def execute_item(self, item):
        yearfiles = [
            os.path.join(US_REFERENCE_PARSED_PATH, x)
            for x in list_dir(US_REFERENCE_PARSED_PATH, ".xml")
            if str(item) in x
        ]
        if self.regulations:
            yearfiles += [
                os.path.join(US_REG_REFERENCE_PARSED_PATH, x)
                for x in list_dir(US_REG_REFERENCE_PARSED_PATH, ".xml")
                if str(item) in x
            ]
        data = []
        for file in yearfiles:
            with open(file, encoding="utf8") as f:
                file_elem = lxml.etree.parse(f)
            for node in file_elem.xpath("//*[@citekey]"):
                data.append([node.attrib["key"], node.attrib["citekey"]])
            if self.detailed_crossreferences:
                for node in file_elem.xpath("//*[@citekey_detailed]"):
                    for citekey in node.attrib["citekey_detailed"].split(","):
                        data.append([node.attrib["key"], citekey])
        df = pd.DataFrame(data, columns=["key", "citekey"])
        destination_file = f"{self.dest}/{get_filename(item)}"
        df.to_csv(destination_file, index=False)


def get_filename(year):
    return f"{year}.csv"
