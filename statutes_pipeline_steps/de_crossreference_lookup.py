import pandas as pd
from quantlaw.utils.beautiful_soup import create_soup
from quantlaw.utils.files import ensure_exists

from statics import (
    DE_CROSSREFERENCE_LOOKUP_PATH,
    DE_REFERENCE_PARSED_PATH,
    DE_REG_CROSSREFERENCE_LOOKUP_PATH,
    DE_REG_REFERENCE_PARSED_PATH,
)
from utils.common import RegulationsPipelineStep, get_snapshot_law_list, load_law_names


class DeCrossreferenceLookup(RegulationsPipelineStep):
    def get_items(self, snapshots) -> list:
        ensure_exists(
            DE_REG_CROSSREFERENCE_LOOKUP_PATH
            if self.regulations
            else DE_CROSSREFERENCE_LOOKUP_PATH
        )
        files = []
        law_names_data = load_law_names(self.regulations)
        for snapshot in snapshots:
            files.append((snapshot, get_snapshot_law_list(snapshot, law_names_data)))
        return files

    def execute_item(self, item):
        date, files = item
        data = []
        source_folder = (
            DE_REG_REFERENCE_PARSED_PATH
            if self.regulations
            else DE_REFERENCE_PARSED_PATH
        )
        target_folder = (
            DE_REG_CROSSREFERENCE_LOOKUP_PATH
            if self.regulations
            else DE_CROSSREFERENCE_LOOKUP_PATH
        )
        for file in files:
            soup = create_soup(f"{source_folder}/{file}")
            for tag in soup.find_all(citekey=True):
                data.append([tag.attrs["key"], tag.attrs["citekey"]])
        df = pd.DataFrame(data, columns=["key", "citekey"])
        destination_file = f"{target_folder}/{date}.csv"
        df.to_csv(destination_file, index=False)
