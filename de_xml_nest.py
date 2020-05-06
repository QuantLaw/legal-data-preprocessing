import os
import re

from bs4 import BeautifulSoup, NavigableString
from lxml import etree

from common import ensure_exists, list_dir, categorize_heading_regex
from statics import DE_XML_NESTED_PATH, DE_XML_PATH, DE_XML_HEADING_ORDERS_PATH


def de_xml_nest_prepare(overwrite):
    ensure_exists(DE_XML_NESTED_PATH)
    files = list_dir(DE_XML_PATH, ".xml")

    if not overwrite:
        existing_files = os.listdir(DE_XML_NESTED_PATH)
        files = list(filter(lambda f: f not in existing_files, files))

    return files


def de_xml_nest(filename, heading_orders):
    with open(f"{DE_XML_PATH}/{filename}") as f:
        text = f.read()
    secs = heading_orders[filename]
    secs = secs or []
    secs = ["Anlage", "Anhang", *secs]  # anlage, anhang currently removed. See below
    secs_patterns = [categorize_heading_regex(x) for x in secs]

    old_soup = BeautifulSoup(text, "lxml-xml")

    soup = BeautifulSoup("", "lxml-xml")
    if old_soup.document.heading:
        heading = old_soup.document.heading.get_text(" ").strip()
    else:
        heading = ""
    item_cursor = soup.new_tag(
        "document",
        attrs={
            "heading": heading,
            "level": 0,
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:noNamespaceSchemaLocation": "../../pipeline/xml-schema.xsd",
        },
    )
    seqitem_cursor = None
    soup.append(item_cursor)

    is_preamble = True
    is_appendix = False
    lawname_heading = True
    for old_tag in old_soup.find_all(["heading", "text"]):
        if old_tag.name == "heading":
            heading_text = old_tag.get_text(" ").strip()

            #########
            # Analyze
            #########

            if lawname_heading:
                lawname_heading = False
                continue

            # Check if item or seqitem
            item_level = None
            for i, secs_pattern in enumerate(secs_patterns):
                if re.match(secs_pattern, heading_text, re.IGNORECASE):
                    item_level = i + 1
                    break

            # Check if seqitem
            is_seqitem = not item_level and re.match(
                r"§(.*?)|Art(ikel)?\.?\s+([0-9]|\b(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})\b)(.*?)",
                heading_text,
                re.IGNORECASE,
            )

            is_appendix = re.match(
                r"(Anlage|Anhang|Schlu.s?formel|Tabelle \w+ zu)\b",
                heading_text,
                re.IGNORECASE,
            )

            if item_level or is_seqitem:
                is_preamble = False

            if not item_level and not is_seqitem and not is_appendix:
                item_level = 1 if "Schlußbestimmungen" in heading_text else 99

            #################
            # Add to new soup
            #################

            if is_preamble or is_appendix:
                continue

            new_tag = soup.new_tag(
                "seqitem" if is_seqitem else "item",
                attrs=dict(level=item_level, heading=heading_text),
            )

            if item_level:
                parent_of_item = get_predecessor_parent_for_level(
                    item_cursor, item_level
                )
                parent_of_item.append(new_tag)
                item_cursor = new_tag
                seqitem_cursor = None

            if is_seqitem:
                item_cursor.append(new_tag)
                seqitem_cursor = new_tag

        else:
            if is_preamble or is_appendix:
                continue
            assert old_tag.name == "text"
            if seqitem_cursor:
                new_tag = soup.new_tag("subseqitem")
                text_tag = soup.new_tag("text")
                new_tag.append(text_tag)
                text_tag.append(NavigableString(old_tag.get_text(" ").strip()))
                seqitem_cursor.append(new_tag)
            else:
                skipping_text = old_tag.get_text().strip()

                # TODO LATER some texts are ignored. However minor fraction of all texts in rather unimportant laws with paticular structure. -
                # eg. BEG, SGBAT, SGBSVVs, SGB-10-Kap1-2, WDNeuOG, 1-BesVNG, 2-BesVNG, BVGSaarEG, HypBkGÄndG, BBankG, MVzAFWoG, IntPatÜbkG
                # For debug
                # if len(skipping_text) > 5 and not (
                #     len(skipping_text) < 30 and "weggefallen" in skipping_text
                # ):
                #     print(
                #         filename,
                #         "|",
                #         heading_text,
                #         "|",
                #         re.sub(r"\n+", " ", skipping_text),
                #     )

    # Clean unnecessary subseqitems
    remove_unnecessary_subseqitems(soup)

    key_prefix = os.path.splitext(filename)[0]
    citekey_prefix = os.path.splitext(filename)[0].split("_")[1]
    remove_removed_items(soup)

    add_keys_and_levels(soup, key_prefix, citekey_prefix)

    with open(f"{DE_XML_NESTED_PATH}/{filename}", "w") as f:
        f.write(str(soup))

    if not validate_xml(str(soup), "xml-schema.xsd"):
        raise Exception(f"Wrong format: {DE_XML_NESTED_PATH}/{filename}")


###########
# Functions
###########


def get_predecessor_parent_for_level(tag, level):
    if tag.attrs["level"] < level:
        return tag
    return get_predecessor_parent_for_level(tag.parent, level)


def remove_unnecessary_subseqitems(soup):
    """ Similar method in US-htmlconverter """
    for seqitem in soup.find_all(["seqitem", "item"]):
        if len(seqitem.contents) == 1 and seqitem.contents[0].name == "subseqitem":
            seqitem.contents[0].unwrap()


def remove_removed_items(soup):
    """
    Remove items, seqitems and subseqitems that were removed
    """
    try:
        _ = remove_removed_items.pattern
    except AttributeError:
        remove_removed_items.pattern = re.compile(
            r".{0,5}(weggefallen|\-|\—|aufgehoben|entf.ll(t|en)|gestrichen|unbesetzt).{0,5}",
            flags=re.IGNORECASE,
        )
    for seqitems in soup.find_all("seqitem"):
        item_text = seqitems.get_text().strip()
        item_length = len(seqitems.get_text().strip())
        if item_length == 0:
            seqitems.decompose()
        elif item_length < 15 and remove_removed_items.pattern.fullmatch(item_text):
            seqitems.decompose()

    items = list(soup.find_all("item"))
    # iterate over soup in reverse item order to ensure empty item removal works
    for item in reversed(items):
        if "weggefallen" in item.attrs.get("heading").lower():
            item.decompose()
        # items without content should go (assuming some variant of "weggefallen")
        elif len(item.contents) == 0:
            item.decompose()


def nodeid_counter():
    nodeid_counter.counter += 1
    return nodeid_counter.counter


def add_keys_and_levels(soup, key_prefix, citekey_prefix):
    nodeid_counter.counter = 0
    for tag in soup.find_all(["document", "item", "seqitem", "subseqitem"]):
        tag.attrs["key"] = f"{key_prefix}_{nodeid_counter():06d}"

        if tag.name != "document":
            tag.attrs["level"] = tag.parent.attrs["level"] + 1

    for tag in soup.find_all(["seqitem"]):
        heading = tag.attrs["heading"]
        match = re.match(r"(§§?|Art[a-z\.]*)\s?(\d+[a-z]*)\b", heading)
        if not match:  # For debug
            print(f"Cannot create citekey of {heading} in {key_prefix}")
            continue
        tag.attrs["citekey"] = f"{citekey_prefix}_{match[2]}"


def get_xml_heading_orders():
    """obtain the mapping from law identifiers to section names"""
    with open(DE_XML_HEADING_ORDERS_PATH) as f:
        entries = [l.strip().split(",") for l in f.readlines()]
    sections = {x[0]: x[1:] for x in entries}
    return sections


def validate_xml(xml: str, xsd_path: str) -> bool:

    xmlschema_doc = etree.parse(xsd_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)

    xml_doc = etree.XML(xml.encode("utf8"))
    result = xmlschema.validate(xml_doc)

    return result
