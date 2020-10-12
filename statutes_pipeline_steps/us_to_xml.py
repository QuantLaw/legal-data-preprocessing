import os
import re
from collections import OrderedDict

import bs4
from bs4 import BeautifulSoup, Tag
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.pipeline import PipelineStep

from statics import US_ORIGINAL_PATH, US_XML_PATH


class UsToXmlStep(PipelineStep):
    def get_items(self, overwrite) -> list:
        # Create target folder
        ensure_exists(US_XML_PATH)

        # Get source files
        files = list_dir(US_ORIGINAL_PATH, ".htm")

        # Filter appendices
        pattern = re.compile(r"\d+0_\d+\.htm")
        html_files = list(filter(pattern.fullmatch, files))

        # Prevent file overwrite
        if not overwrite:
            existing_files = list_dir(US_XML_PATH, ".xml")
            existing_files_sources = list(
                map(lambda x: x.replace(".xml", ".htm"), existing_files)
            )

            html_files = list(
                filter(lambda f: f not in existing_files_sources, html_files)
            )

        return html_files

    def execute_item(self, item):
        filepath = f"{US_ORIGINAL_PATH}/{item}"

        # get snapshot
        match = re.fullmatch(r"(\d+)_(\d+)\.htm", item)
        snapshot = match[2]

        # Read the source file
        soup = htm_to_soup(filepath)

        # Split into documents (roughly sections)
        documents = split_into_documents(soup)

        # Create a nested structure of sections
        roots = nest_documents(documents)

        # Create a nested structure below section level
        for document in documents:
            convert_statute_field_to_contents(document)

        # Generate xml and save
        export_to_xml(roots, snapshot)


#################
# Load html files
#################


def htm_to_soup(filename: str) -> BeautifulSoup:
    """
    Opens and parses a file.
    """
    with open(filename, "rb") as f:
        lines = f.readlines()

    # Cleanup some errors manually
    correct_errors_in_source(filename, lines)

    # Remove weird formatting
    content = b"".join(lines).decode("utf-8-sig", errors="ignore")
    content = content.replace("\x1a", "")

    # Parse file
    soup = BeautifulSoup(content, "lxml")
    return soup


def correct_errors_in_source(filepath, lines):
    """
    Modifies a file to correct errors in the source
    Args:
        filename: relative path to the file
        lines: array of lines of the file that should be corrected.
            This array will be modified.
    """

    filename = os.path.split(filepath)[1]
    match = re.fullmatch(r"(\d+)_(\d+).htm", filename)
    assert match
    title, year = match[1], match[2]

    if title == "400" and year == "2008":  # remove double closing </div> tag
        assert lines[23550] == b'<div class="analysis-head-right">Sec.</div></div>\r\n'
        lines[23550] = b'<div class="analysis-head-right">Sec.</div>\r\n'

    elif title == "420" and year in [
        "1995",
        "1994",
    ]:  # remove double closing </div> tag
        replace_indexes = list(
            all_indices_of_object(
                b'<div class="analysis-head-right">'
                b"<em>Amount&nbsp;&nbsp;</em>"
                b"</div></div>\r\n",
                lines,
            )
        )
        assert len(replace_indexes) == 6
        for replace_indexe in replace_indexes:
            lines[replace_indexe] = (
                b'<div class="analysis-head-right">'
                b"<em>Amount&nbsp;&nbsp;</em>"
                b"</div>\r\n"
            )

    elif title == "281" and year == "2017":  # remove inconsistent document
        # filename == "2017usc28a.htm"
        assert str(lines[3468]) == (
            "b'<!-- documentid:28a_-FEDERAL_RULES_OF_APPELLATE_PROCEDURE-"
            "&#160;&#160; currentthrough:20180112 documentPDFPage:92 -->\\r\\n'"
        )
        del lines[3468:3475]

    elif title == "110" and year == "1998":  # remove title 12 from title 11 file
        start_line = None
        end_line = None
        for idx, line in enumerate(lines):
            line_str = line.decode("utf-8")
            if line_str.startswith("<!-- documentid:11_ "):
                start_line = idx
            if line_str.startswith("<!-- documentid:11_-TITLE_11 "):
                end_line = idx
            if "<!-- itempath:/TITLE 11" in line_str:
                new_line = line_str.replace(
                    "<!-- itempath:/TITLE 11", "<!-- itempath:/110"
                ).encode("utf-8")
                lines[idx] = new_line
        assert start_line and end_line
        del lines[start_line:end_line]

    for i in range(len(lines)):
        if lines[i] == b"<!-- /section-head -->\r\n" or lines[i] == b"<statute>\r\n":
            print(f"Fix {i} in {title}/{year}: {str(lines[i])}")
            lines[i] = b""


def all_indices_of_object(target, my_list):
    """
    Returns: array of indices where the target is in my_list
    """
    for i in range(len(my_list)):
        if my_list[i] == target:
            yield i


#################
# Load html files
#################


def split_into_documents(soup):
    remove_elements(soup, "sup")

    docs = []

    # This fires especially if there are too many </div> tags in the content
    assert [type(o) for o in soup.body.children] == [
        bs4.element.NavigableString,
        bs4.element.Tag,
    ]
    doc_properties = (
        {}
    )  # Filled with properties of html comments. Cleared when beginning new document.
    # Filled with tags between 'field-start' and 'field-start' comments.
    # Cleared when beginning new document.
    doc_fields = {}

    open_fields = []  # Filled with open tags, using 'field-start' as start marker and
    # 'field-start' as end marker.
    last_tag = None
    introduction = (
        True  # True if the iterator has recognized the beginning of the first document
    )

    for tag in soup.body.div.contents:

        # Skipping introduction
        if introduction:
            introduction = not introduction_ends(tag)
        if introduction:
            continue

        if type(tag) is bs4.element.NavigableString:
            if not tag.string.strip() == "":
                assert last_tag
                last_tag.append(
                    " " + tag.string.strip()
                )  # Strings not in a tag a added to the last tag

        elif type(tag) is bs4.element.Comment:

            comment_key, comment_value = analyze_comment(tag)

            if comment_key == "documentid":  # Close document and create new one
                close_and_new_document(doc_properties, doc_fields, docs)
                doc_properties = {}
                doc_fields = {}

            if comment_key in {"documentid", "expcite", "itemsortkey", "itempath"}:
                doc_properties[comment_key] = comment_value
            elif comment_key == "field-start":
                open_fields.append(comment_value)
            elif comment_key == "field-end":
                field_closed = open_fields.pop()
                assert field_closed == comment_value

            assert_comment_format(tag, open_fields, doc_properties)

        elif type(tag) is bs4.element.Tag:
            assert len(open_fields) > 0  # Tag is enclosed by at least one field
            fields_path = "/".join(open_fields)
            if fields_path not in doc_fields:  # Start new field
                doc_fields[fields_path] = []
            doc_fields[fields_path].append(tag)
            last_tag = tag

            # Handle field-start and field-end if nested in a tag
            for descendant in tag.descendants:
                if type(descendant) is bs4.element.Comment:
                    comment_components = descendant.string.strip().split(":")
                    comment_key = comment_components[0]
                    comment_value = ":".join(comment_components[1:])
                    if comment_key == "field-start":
                        open_fields.append(comment_value)
                    elif comment_key == "field-end":
                        field_closed = open_fields.pop()
                        assert field_closed == comment_value
        else:
            raise Exception(f"Unknown item type: {tag}")

    close_and_new_document(doc_properties, doc_fields, docs)  # Close last document
    return docs


def remove_elements(soup, tag_name):
    """
    Removes all elements with a given tage name from the tree
    """
    tags = list(soup.find_all(tag_name))
    for tag in tags:
        tag.extract()


def introduction_ends(element) -> bool:
    """
    Checks weather the given element indicates that the introduction of the statute
    ends
    """
    return (
        type(element) is bs4.element.Comment
        and analyze_comment(element)[0] == "documentid"
    )


def close_and_new_document(doc_properties, doc_fields, docs):
    if "documentid" in doc_properties:
        docs.append({**doc_properties, "fields": doc_fields})


def assert_comment_format(comment, open_fields, doc_properties):
    """
    Validates the format of the comment
    """
    comment_key, comment_value = analyze_comment(comment)

    if comment_key not in {
        "documentid",
        "expcite",
        "itempath",
        "itemsortkey",
        "field-start",
        "field-end",
        "PDFPage",
    }:
        raise Exception("Unknown Comment: " + comment)

    if comment_key in ["PDFPage", "field-start", "field-end"]:
        assert len(comment.string.strip().split(":")) == 2  # No ':' in field name
    elif comment_key == "field-start":
        # All properties set above first field
        assert set(doc_properties.keys()) == {
            "documentid",
            "expcite",
            "itempath",
            "itemsortkey",
        }
    elif comment_key == "documentid":
        assert open_fields == []  # All fields closed, when opening new document
    elif comment_key == "field-start":
        assert "/" not in comment_value, comment_value


def analyze_comment(comment) -> tuple:
    """
    Extracts content from a comment
    """
    assert type(comment) is bs4.element.Comment
    comment_components = comment.string.strip().split(":")
    return comment_components[0], ":".join(comment_components[1:])


#######################################
# Nest documents
#######################################


def nest_documents(documents):
    """
    Use itempathcomponents attribute of each document to nest the provided documents
    """

    for document in documents:
        add_pathcomponents(document)
    docs_by_itempath = {doc["itempathcomponents"]: doc for doc in documents}
    roots = []
    for doc in documents:
        assert len(doc["itempathcomponents"]) > 0

        if len(doc["itempathcomponents"]) == 1:
            # Append to root
            roots.append(doc)

        else:
            # For elements ar lower levels (create and) get parent to add the document
            # to.
            parent = get_or_create_parent(docs_by_itempath, doc)
            parent["children"].append(doc)

    return roots


def add_pathcomponents(document):
    """
    Analyze document attributes and add another attribute for path components and
    an empty list for possible children.
    """
    itempathcomponents = document["expcite"].split("!@!")
    assert len(itempathcomponents) > 0
    itempathcomponents[0] = document["itempath"].split("/")[1]
    document["itempathcomponents"] = tuple(itempathcomponents)
    document["children"] = []


def get_or_create_parent(docs_by_itempath, doc):
    """
    Get the parent for a document based on the itempathcomponents. If the parent does
    not exist yet it will be created.
    """
    parent = docs_by_itempath.get(doc["itempathcomponents"][:-1])

    if not parent:
        grandparent = docs_by_itempath[doc["itempathcomponents"][:-2]]
        parent = {
            "itempath": "/".join(doc["itempath"].split("/")[:-1]),
            "expcite": "!@!".join(doc["expcite"].split("!@!")[:-1]),
            "fields": {},
        }
        add_pathcomponents(parent)
        docs_by_itempath[parent["itempathcomponents"]] = parent
        grandparent["children"].append(parent)

    return parent


#######################################
# Clean statute fields
#######################################

# maps html classes to element types
# Order of the mappings indicates the hierarchy of types
# Values of the same type must be assigned to consecutive keys.
STATUTE_STRUCTURE = OrderedDict(
    [
        # items with title and content
        ("subsection-head", "subsection"),
        ("paragraph-head", "paragraph"),
        ("subparagraph-head", "subparagraph"),
        ("clause-head", "clause"),
        ("subclause-head", "subclause"),
        ("subsubclause-head", "subsubclause"),
        ("analysis-subhead", "subhead"),  # discretion
        ("item-head", "item"),  # discretion
        # items with content only
        ("statutory-body", "text"),
        ("usc28aForm-left", "text"),  # discretion
        ("usc28aform-right", "text"),  # discretion
        ("signature", "text"),  # discretion
        ("presidential-signature", "text"),  # discretion
        ("note-body", "text"),  # discretion
        ("note-body-small", "text"),  # discretion
        ("note-body-block", "text"),  # discretion, maybe text-misc instead
        ("italic-text-block", "text"),  # discretion
        ("statutory-body-1em", "text-enumeration1"),
        ("note-body-1em", "text-enumeration1"),  # discretion
        ("statutory-body-2em", "text-enumeration2"),
        ("note-body-2em", "text-enumeration2"),  # discretion
        ("statutory-body-3em", "text-enumeration3"),
        ("note-body-3em", "text-enumeration3"),  # discretion
        ("statutory-body-4em", "text-enumeration4"),
        ("statutory-body-5em", "text-enumeration5"),
        ("statutory-body-6em", "text-enumeration6"),
        ("formula", "text-misc"),  # discretion
        ("note-sub-head", "text-misc"),  # discretion
        ("subchapter-head", "text-misc"),  # discretion
        ("tableftnt", "text-misc"),  # discretion
        ("note-body-flush3_hang4", "text-misc"),  # discretion
        ("5802I62", "text-misc"),  # discretion
        ("5802I63", "text-misc"),  # discretion
        ("5801I76", "text-misc"),  # discretion
        ("5800I01", "text-misc"),  # discretion
        ("5803I01", "text-misc"),  # discretion
        ("5801I01", "text-misc"),  # discretion
        ("5801I05", "text-misc"),  # discretion
        ("italic-text-para", "text-misc"),  # discretion
        ("statutory-body-flush0_hang1", "text-misc"),  # discretion
        ("dispo", "text-misc"),  # discretion
    ]
)

STATUTE_STRUCTURE_VALUES = list(STATUTE_STRUCTURE.values())

# mappings a text block to an element type. Normally this belongs to previous item.
# E.g. This is a sentence
#      1. with an list item
#      2. and another
#      and this is the continuation of the initial sencente after the enumeration.
CONTINUATIONS = {
    "statutory-body-block": "text",
    "statutory-body-block-1em": "text-enumeration1",
    "note-body-block-1em": "text-enumeration1",
    "statutory-body-block-2em": "text-enumeration2",
    "statutory-body-block-4em": "text-enumeration4",
    "statutory-body-flush0_hang2": "text-enumeration2",  # discretion
    "statutory-body-flush2_hang3": "text-enumeration3",  # discretion
    "statutory-body-flush2_hang4": "text-enumeration4",  # discretion
}


# classes that are ignored in further process.
SKIP_CLASS = {
    # /281/FEDERAL RULES OF APPELLATE PROCEDURE/APPENDIX OF FORMS/Form 4
    "rules-form-source-credit",
    "analysis-style-table",
    "leader-work-left",
    "leader-work-right",
    "leader-work-head-left",
    "leader-work-head-right",
    "italic-head",
    "two-column-analysis-style-content-left",
    None,
}

ALLOWED_SUBTAGS = ["i", "sup", "a", "cap-smallcap", "strong", "sub", "em", "br"]


def assert_statutes_child(child):
    """
    Validates that the document content contains only expected elements.
    """
    assert (
        not child.get("class") or len(child.get("class")) == 1
    )  # Max. one class per child
    # Assertion: allowed subtags
    if child.name in ["h4", "p", "h3"]:
        for descendant in child.descendants if child.descendants else []:
            # only contains string or comment elements or
            # if comment tag, allow only allowed tag.names
            assert type(descendant) in [
                bs4.element.NavigableString,
                bs4.element.Comment,
            ] or (
                type(descendant) is bs4.element.Tag
                and descendant.name in ALLOWED_SUBTAGS
            )


def check_allowed_div_table_subtags(tag) -> bool:
    """
    Checks that the tag only contains expected children
    """
    for descendant in tag.descendants if tag.descendants else []:
        if not (
            type(descendant) in [bs4.element.Comment, bs4.element.NavigableString]
            or descendant.name
            in {
                *ALLOWED_SUBTAGS,
                "div",
                "table",
                "tr",
                "td",
                "th",
                "caption",
                "p",
                "br",
            }
        ):
            print(descendant)
            return False
    return True


def tag_to_string(tag):
    """
    Converts a tag to string and ensures that the contents of cells in a table are
    separated by a space
    """
    tag = BeautifulSoup(str(tag), "lxml")
    for tag in [*tag.findAll("th"), *tag.findAll("td")]:
        tag.contents.append(bs4.element.NavigableString(" "))
    return re.sub(r"\s+", " ", BeautifulSoup(str(tag), "lxml").text.strip())


def get_subitem_path(document, open_elements, current_title):
    assert open_elements[0]["type"] == "section"
    section_path_elements = [
        elem["path_component"].split()[0] for elem in open_elements[1:]
    ]
    last_path_component = current_title.split()[0]
    path = "/".join(
        [*document["itempathcomponents"], *section_path_elements, last_path_component]
    )

    return path


def convert_statute_field_to_contents(document):
    """
    Converts the contents of a document which is in this case a section.
    """
    if not document["fields"].get("statute"):
        return

    document["contents"] = {"type": "section", "contents": []}
    open_elements = [document["contents"]]

    for child in document["fields"]["statute"]:
        if child.name in ["img", "br"]:
            continue  # Skip images and line breaks
        assert_statutes_child(child)
        child_class = child["class"][0] if child.get("class") else None  # Unpack class

        # Generate a list with open list types (without table)
        open_element_types = [elem["type"] for elem in open_elements]

        if child.name in ["div", "table"] and child_class not in [
            o for o in SKIP_CLASS if o
        ]:
            assert check_allowed_div_table_subtags(child)
            string = tag_to_string(child)
            new_element = {"type": "table", "contents": string}
            open_elements[-1]["contents"].append(new_element)
            # do not add to open_elements , as other elements cannot be nested in table

        elif child_class in STATUTE_STRUCTURE:  # default way of adding new elements
            child_type = STATUTE_STRUCTURE[child_class]

            # close elements that are of the same or a lower level.
            # Order is determined by: STATUTE_STRUCTURE / STATUTE_STRUCTURE_VALUES
            # get type of current element and all lower types
            types_to_remove = STATUTE_STRUCTURE_VALUES[
                STATUTE_STRUCTURE_VALUES.index(child_type) :
            ]
            remove_from = None
            for open_element_type in open_element_types:
                if open_element_type in types_to_remove:
                    # get highest element in hierarchy that must be closed
                    # by removing it from open_elements
                    remove_from = open_element_type
                    break
            if remove_from:
                del open_elements[
                    open_element_types.index(remove_from) :
                ]  # close / remove respecive elements

            # create new element
            if child_type.startswith("text"):
                new_element = {"type": child_type, "contents": [child.get_text()]}
            else:
                new_element = {
                    "type": child_type,
                    "title": child.get_text(),
                    "contents": [],
                    "path_component": child.get_text(),
                    "path": get_subitem_path(document, open_elements, child.get_text()),
                }
            open_elements[-1]["contents"].append(new_element)  # add to parent
            open_elements.append(
                new_element
            )  # open element to enable adding children to it

        elif child_class in CONTINUATIONS:  # add text to previously opened element
            # e.g. [beginning of sentence] [enumeration] [END OF SENTENCE].
            continued_item_type = CONTINUATIONS[child_class]

            # In some cases, text of that element only appears after the enumeration.
            # In this case the omitted element must be added around the enumeration
            if continued_item_type not in open_element_types:
                # get types that should be nested in current type
                lower_types = STATUTE_STRUCTURE_VALUES[
                    STATUTE_STRUCTURE_VALUES.index(continued_item_type) + 1 :
                ]
                subelement_type = None
                for lower_type in lower_types:
                    if lower_type in open_element_types:
                        subelement_type = lower_type
                        break

                # No subtypes are found to nest: Append without nesting
                # Single case in 2017: 2017usc /180/PART I/CHAPTER 2/Sec. 33.
                if not subelement_type:
                    previous_element = open_elements.pop()
                    new_element = {
                        "type": previous_element["type"],
                        "contents": [child.get_text()],
                    }
                    previous_element["contents"].append(new_element)
                    open_elements.append(new_element)
                    continue

                lower_level = open_element_types.index(
                    subelement_type
                )  # get level of subelements
                contents_to_move = open_elements[lower_level - 1][
                    "contents"
                ]  # get subelements
                # create a new element containing subelements
                interim_element = {
                    "type": continued_item_type,
                    "contents": contents_to_move,
                }
                open_elements[lower_level - 1]["contents"] = [
                    interim_element
                ]  # insert new element into tree
                open_elements.insert(lower_level, interim_element)  # open new element
                open_element_types = [
                    elem["type"] for elem in open_elements
                ]  # regenerate open_element_types

            # Close subelements and continue element of current level
            # This is the only actions required,
            # if the continued element already exists in the tree
            parent_level = open_element_types.index(continued_item_type)
            del open_elements[parent_level + 1 :]
            open_elements[-1]["contents"].append(child.get_text())
            # Raise an error if class in unknown and not in skipped

        else:
            if (
                str(child) != '<td class="middle"></td>'
                and str(child) != '<td class="right"></td>'
                and not (child_class in SKIP_CLASS)
            ):
                raise Exception(child)


########
# Export
########


def doc_to_soup(doc, soup, level, version, root=False) -> Tag:
    """
    Converts the intermediate structure to a xml soup
    """
    tag_name = "document" if root else ("item" if len(doc["children"]) else "seqitem")
    tag = soup.new_tag(tag_name, level=level, heading=doc["expcite"].split("!@!")[-1])
    if tag_name == "seqitem":
        title_path_component = doc["itempath"].split("/")[1]
        if title_path_component[-1] == "0":
            title = doc["expcite"].split("-")[0].split()[-1]
            section = doc["itempath"].split()[-1]
            tag.attrs["citekey"] = f"{title}_{section}"
        # else:
        #    assert title_path_component[-1] == '1'
    for content in doc["contents"]["contents"] if doc["fields"].get("statute") else []:
        tag.append(content_to_soup(content, soup, level + 1, version, doc))
    for child in doc["children"]:
        tag.append(doc_to_soup(child, soup, level + 1, version))
    return tag


def content_to_soup(content, soup, level, version, doc) -> Tag:
    """
    Converts the intermediate structure within a document to a xml soup
    """
    if type(content) is dict:
        tag = soup.new_tag("subseqitem", level=level, heading=content.get("title", ""))

        for subcontent in content["contents"]:
            tag.append(content_to_soup(subcontent, soup, level + 1, version, doc))
        return tag

    elif type(content) is str:
        text = soup.new_tag("text")
        text.append(bs4.element.NavigableString(content))
        return text
    else:
        raise Exception(type(content))


def remove_unnecessary_subseqitems(soup):
    """
    Cleans the soup from unnecessary items, such as subseqitems if a seqitem only
    contains one subseqitem.
    """
    for seqitem in soup.find_all("seqitem"):
        if (
            len(seqitem.contents) == 1
            and seqitem.contents[0].name == "subseqitem"
            and seqitem.contents[0].attrs["heading"] == ""
            and len(seqitem.contents[0].contents) == 1
            and seqitem.contents[0].contents[0].name == "text"
        ):
            seqitem.contents[0].unwrap()


def nodeid_counter():
    """
    Get next counter number
    """
    nodeid_counter.counter += 1
    return nodeid_counter.counter


def add_keys_to_items(soup, prefix):
    """
    Add attrs to tags in soup
    """
    nodeid_counter.counter = 0
    for tag in soup.find_all(["document", "item", "seqitem", "subseqitem"]):
        tag.attrs["key"] = f"{prefix}_{nodeid_counter():06d}"


def export_to_xml(roots, version):
    """
    Converts the intermediate structure to a soup and saves the xml
    """
    for root in roots:
        with open(f'{US_XML_PATH}/{root["itempath"][1:]}_{version}.xml', "wb") as f:
            soup = BeautifulSoup("", "lxml")
            soup.append(doc_to_soup(root, soup, 0, version, root=True))
            remove_unnecessary_subseqitems(soup)
            add_keys_to_items(soup, f'{root["itempathcomponents"][0]}_{version}')
            f.write(soup.encode("utf-8"))
