import os

import networkx as nx
import pandas as pd
from quantlaw.utils.files import ensure_exists, list_dir

from utils.common import RegulationsPipelineStep, get_snapshot_law_list, load_law_names


class CrossreferenceGraphStep(RegulationsPipelineStep):
    def __init__(
        self,
        source,
        destination,
        edgelist_folder,
        dataset,
        authority_edgelist_folder,
        *args,
        **kwargs,
    ):
        self.source = source
        self.destination = destination
        self.edgelist_folder = edgelist_folder
        self.dataset = dataset
        self.authority_edgelist_folder = authority_edgelist_folder
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite, snapshots) -> list:
        ensure_exists(self.destination)
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
            existing_files = list_dir(self.destination, ".gpickle.gz")
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
                files.append(
                    (
                        snapshot,
                        [
                            f"{self.source}/{x}"
                            for x in os.listdir(self.source)
                            if str(snapshot) in x
                        ],
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
                            f'{self.source}/{x.replace(".xml", ".graphml")}'
                            for x in graph_files
                        ],
                    )
                )

        return files

    def execute_item(self, item):
        year, files = item

        # make forest from trees
        edge_list = pd.read_csv(f"{self.edgelist_folder}/{year}.csv")
        G = nx.MultiDiGraph()
        for file in files:
            print(file)
            nG = nx.read_graphml(file)

            # enable filtering by law name
            nx.set_node_attributes(nG, nG.graph.get("name", file), name="law_name")
            G.add_nodes_from(nG.nodes(data=True))
            G.add_edges_from(nG.edges())
        G = nx.MultiDiGraph(G)

        # root means "U.S.C." for US and "Bundesgesetze" for DE (umbrella node)
        G.add_node("root", level=-1, key="root", law_name="root")

        # Add edges from root to the roots to titles (US) or laws (DE)
        for root in [r for r in G.nodes() if G.nodes[r]["level"] == 0]:
            G.add_edge("root", root, edge_type="containment")
        nx.set_edge_attributes(G, "containment", name="edge_type")

        # Get reference edges
        edges = [tuple(edge[1].values) for edge in edge_list.iterrows()]

        # Assert that no new nodes will be added by the edges
        for node_from, node_to in edges:
            assert G.has_node(node_from)
            assert G.has_node(node_to)

        # Add reference edges
        G.add_edges_from(edges, edge_type="reference")

        # add authority edges
        if self.regulations:
            edge_list = pd.read_csv(f"{self.authority_edgelist_folder}/{year}.csv")
            edges = [tuple(edge[1].values) for edge in edge_list.iterrows()]
            for node_from, node_to in edges:
                assert G.has_node(node_from)
                assert G.has_node(node_to)
            G.add_edges_from(edges, edge_type="authority")

        G.graph["name"] = f"{year}"

        # Save
        nx.write_gpickle(G, f"{self.destination}/{year}.gpickle.gz")
