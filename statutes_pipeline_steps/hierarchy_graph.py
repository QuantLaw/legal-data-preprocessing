import os
import re

import networkx as nx
from lxml import etree
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.pipeline import PipelineStep


class HierarchyGraphStep(PipelineStep):
    def __init__(self, source, destination, add_subseqitems, *args, **kwargs):
        self.source = source
        self.destination = destination
        self.add_subseqitems = add_subseqitems
        super().__init__(*args, **kwargs)

    def get_items(self, overwrite) -> list:
        ensure_exists(self.destination)
        files = list_dir(self.source, ".xml")

        if not overwrite:
            existing_files = list_dir(self.destination, ".gpickle")
            files = list(
                filter(lambda f: get_gpickle_filename(f) not in existing_files, files)
            )

        return files

    def execute_item(self, item):
        G = build_graph(f"{self.source}/{item}", add_subseqitems=self.add_subseqitems)

        destination_path = f"{self.destination}/{get_gpickle_filename(item)}"
        nx.write_gpickle(G, destination_path)


###########
# Functions
###########


def get_gpickle_filename(filename):
    return f"{os.path.splitext(filename)[0]}.gpickle"


def add_juris_attrs(item, node_attrs):
    if item.attrib.get("normgeber"):
        node_attrs["legislators"] = item.attrib["normgeber"]
    if item.attrib.get("mitwirkende"):
        node_attrs["contributors"] = item.attrib["mitwirkende"]
    if item.attrib.get("sachgebiete"):
        node_attrs["subject_areas"] = item.attrib["sachgebiete"]


def nest_items(G, items, document_type):
    """
    Convert xml soup to graph tree using networkx
    """
    for item in items:
        if item.tag != "document":
            node_attrs = dict(
                key=item.attrib["key"],
                citekey=item.attrib.get("citekey", ""),
                heading=item.attrib.get("heading", ""),
                parent_key=item.getparent().attrib["key"],
                level=int(item.attrib["level"]),
                type=item.tag,
            )
            if document_type:
                node_attrs["document_type"] = document_type
            add_juris_attrs(item, node_attrs)

            G.add_node(item.attrib["key"], **node_attrs)
            G.add_edge(item.getparent().attrib["key"], item.attrib["key"])

        else:  # handle root node

            node_attrs = dict(
                key=item.attrib["key"],
                citekey=item.attrib.get("citekey", ""),
                heading=item.attrib.get("heading", ""),
                parent_key="",
                level=0,
                type=item.tag,
                **(dict(document_type=document_type) if document_type else {}),
            )
            if "abbr_1" in item.attrib:
                node_attrs["abbr_1"] = item.attrib["abbr_1"]
            if "abbr_2" in item.attrib:
                node_attrs["abbr_2"] = item.attrib["abbr_2"]
            add_juris_attrs(item, node_attrs)

            G.add_node(item.attrib["key"], **node_attrs)
            G.graph["name"] = item.attrib.get("heading", "")

    return G


def count_characters(text, whites=False):
    """
    Get character count of a text

    Args:
        whites: If True, whitespaces are not counted
    """
    if whites:
        return len(text)
    else:
        return len(re.sub(r"\s", "", text))


def count_tokens(text, unique=False):
    """
    Get token count of given text. Tokens are delimited by whitespaces.
    Args:
        unique: It True, only unique tokens are counted.
    """
    if not unique:
        return len(text.split())
    else:
        return len(set(text.split()))


def build_graph(filename, add_subseqitems=False):
    """
    Builds an awesome graph from a file.
    """

    # Read input file
    tree = etree.parse(filename)

    document_type = (
        tree.xpath("/document")[0].attrib.get("document_type", None)
        if tree.xpath("/document")
        else None
    )

    # Create target graph
    G = nx.DiGraph()

    xpath = (
        "//document | //item | //seqitem | //subseqitem"
        if add_subseqitems
        else "//document | //item | //seqitem"
    )

    # Create a tree if the elements in the target graph
    G = nest_items(G, items=tree.xpath(xpath), document_type=document_type)

    # Add attributes regarding the contained text to the target graoh
    for item in tree.xpath(xpath):
        text = " ".join(item.itertext())
        G.nodes[item.attrib["key"]]["chars_n"] = count_characters(text, whites=True)
        G.nodes[item.attrib["key"]]["chars_nowhites"] = count_characters(
            text, whites=False
        )
        G.nodes[item.attrib["key"]]["tokens_n"] = count_tokens(text, unique=False)
        G.nodes[item.attrib["key"]]["tokens_unique"] = count_tokens(text, unique=True)

    items_with_text = {elem.getparent() for elem in tree.xpath("//text")}
    for item in items_with_text:
        all_elems = item.getchildren()
        text_elems = [e for e in all_elems if e.tag == "text"]
        if len(all_elems) > 1 and text_elems:
            texts_tokens_n = []
            texts_chars_n = []
            for elem in text_elems:
                text = " ".join(elem.itertext())
                texts_tokens_n.append(str(count_tokens(text, unique=False)))
                texts_chars_n.append(str(count_characters(text, whites=False)))
            G.nodes[item.attrib["key"]]["texts_tokens_n"] = ",".join(texts_tokens_n)
            G.nodes[item.attrib["key"]]["texts_chars_n"] = ",".join(texts_chars_n)

    return G
