import multiprocessing
import os

import networkx as nx
import pandas as pd
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.networkx import load_graph_from_csv_files

from utils.common import RegulationsPipelineStep, get_snapshot_law_list, load_law_names


class CrossreferenceGraphStep(RegulationsPipelineStep):
    max_number_of_processes = min(2, max(multiprocessing.cpu_count() - 2, 1))

    def __init__(
        self,
        source,
        source_regulation,
        destination,
        edgelist_folder,
        dataset,
        authority_edgelist_folder,
        *args,
        **kwargs,
    ):
        self.source = source
        self.source_regulation = source_regulation
        self.destination = destination
        self.edgelist_folder = edgelist_folder
        self.dataset = dataset
        self.authority_edgelist_folder = authority_edgelist_folder
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(self.destination + "/seqitems")
        if not snapshots:
            snapshots = sorted(
                set(
                    [
                        os.path.splitext(x)[0]
                        for x in list_dir(self.edgelist_folder, ".csv")
                    ]
                )
            )

        if not overwrite:
            existing_files = list_dir(
                os.path.join(self.destination, "seqitems"), ".gpickle.gz"
            )
            snapshots = list(
                filter(
                    lambda year: f"{year}.gpickle.gz" not in existing_files, snapshots
                )
            )

        if not len(snapshots):
            return []

        if self.dataset == "us":
            files = []
            for snapshot in snapshots:
                statute_files = [
                    f"{self.source}/subseqitems/{x}"
                    for x in os.listdir(os.path.join(self.source, "subseqitems"))
                    if str(snapshot) in x
                ]
                regulation_files = (
                    [
                        f"{self.source_regulation}/subseqitems/{x}"
                        for x in os.listdir(
                            os.path.join(self.source_regulation, "subseqitems")
                        )
                        if str(snapshot) in x
                    ]
                    if self.regulations
                    else None
                )
                files.append(
                    (
                        snapshot,
                        statute_files,
                        regulation_files,
                    )
                )
        else:  # is DE
            files = []
            law_names_data = load_law_names(self.regulations)
            for snapshot in snapshots:
                graph_files = get_snapshot_law_list(snapshot, law_names_data)
                files.append(
                    (
                        snapshot,
                        [
                            f'{self.source}/subseqitems/{x.replace(".xml", ".gpickle")}'
                            for x in graph_files
                        ],
                        None,
                    )
                )

        return files

    def execute_item(self, item):
        year, files, files_regulations = item

        if self.regulations and files_regulations:
            files += files_regulations

        node_columns = [
            "key",
            "level",
            "citekey",
            "parent_key",
            "type",
            "document_type",
            "heading",
            "law_name",
            "chars_n",
            "chars_nowhites",
            "tokens_n",
            "tokens_unique",
            "abbr_1",
            "abbr_2",
            "subject_areas",
            "legislators",
            "contributors",
        ]
        edge_columns = ["u", "v", "edge_type"]

        nodes_csv_path = f"{self.destination}/{year}.nodes.csv.gz"
        edges_csv_path = f"{self.destination}/{year}.edges.csv.gz"

        pd.DataFrame(
            [dict(level=-1, key="root", law_name="root")], columns=node_columns
        ).to_csv(
            nodes_csv_path,
            header=True,
            index=False,
            columns=node_columns,
        )

        pd.DataFrame([], columns=edge_columns).to_csv(
            edges_csv_path,
            header=True,
            index=False,
            columns=edge_columns,
        )

        for file in files:
            nG = nx.read_gpickle(file)
            nx.set_node_attributes(nG, nG.graph.get("name", file), name="law_name")

            nodes_df = pd.DataFrame(
                [d for n, d in nG.nodes(data=True)], columns=node_columns
            )

            if self.dataset.lower() == "us":
                nodes_df["document_type"] = [
                    "regulation" if key.startswith("cfr") else "statute"
                    for key in nodes_df.key
                ]

            nodes_df.to_csv(
                nodes_csv_path,
                header=False,
                index=False,
                columns=node_columns,
                mode="a",
            )

            edges_df = pd.DataFrame(
                [dict(u=u, v=v, edge_type="containment") for u, v in nG.edges()],
                columns=edge_columns,
            )

            for idx, row in nodes_df[nodes_df.level == 0].iterrows():
                edges_df = edges_df.append(
                    [dict(u="root", v=row.key, edge_type="containment")]
                )

            edges_df.to_csv(
                edges_csv_path,
                header=False,
                index=False,
                columns=edge_columns,
                mode="a",
            )

        # Get reference edges
        edge_list = pd.read_csv(f"{self.edgelist_folder}/{year}.csv")
        edges_df = pd.DataFrame(
            {"u": edge_list.out_node, "v": edge_list.in_node, "edge_type": "reference"},
            columns=edge_columns,
        )
        edges_df.to_csv(
            edges_csv_path,
            header=False,
            index=False,
            columns=edge_columns,
            mode="a",
        )

        # add authority edges
        if self.regulations:
            edge_list = pd.read_csv(f"{self.authority_edgelist_folder}/{year}.csv")
            edges_df = pd.DataFrame(
                {
                    "u": edge_list.out_node,
                    "v": edge_list.in_node,
                    "edge_type": "authority",
                },
                columns=edge_columns,
            )
            edges_df.to_csv(
                edges_csv_path,
                header=False,
                index=False,
                columns=edge_columns,
                mode="a",
            )

        # Create and save seqitem graph
        G = load_graph_from_csv_files(
            self.destination, year, filter="exclude_subseqitems"
        )

        nx.write_gpickle(G, f"{self.destination}/seqitems/{year}.gpickle.gz")
