import os
import re
import string
import zipfile
from collections import defaultdict, deque

import lxml.etree
from quantlaw.utils.files import list_dir
from quantlaw.utils.pipeline import PipelineStep
from regex import regex

from download_us_reg_data import ensure_exists
from statics import US_REG_INPUT_PATH, US_REG_XML_PATH

CONTAINER_TAG_SET = [
    "TITLE",
    "SUBTITLE",
    "CHAPTER",
    "SUBCHAPTER",
    "SUBCHAP",
    "PART",
    "SUBPART",
    "SUBJGRP",
]


class UsRegsToXmlStep(PipelineStep):
    def get_items(self, overwrite) -> list:
        # Create target folder
        ensure_exists(US_REG_XML_PATH)

        # Get source files
        files = list_dir(US_REG_INPUT_PATH, ".zip")

        return files

    def execute_item(self, item):
        file_path = os.path.join(US_REG_INPUT_PATH, item)
        parse_cfr_zip(file_path)


extract_text_pattern = re.compile(r"\s+")


def extract_text(e):
    """
    Extract normalized text from an element.
    :param e:
    :return:
    """
    e_text = lxml.etree.tostring(e, encoding="utf-8", method="text").decode("utf-8")
    e_text = extract_text_pattern.sub(" ", e_text)
    return e_text


def extract_number_match(text):
    if len(text) == 0:
        return None, None

    match = re.match(r"\((ii?i?|xx?x?|vv?)\)", text)
    if match:
        return ("alpha-lower-double-bracket", "roman-lower-double-bracket"), match

    match = re.match(r"\((([A-Z])\2*)\)", text)
    if match:
        return "alpha-upper-double-bracket", match

    match = re.match(r"\((([a-z])\2*)\)", text)
    if match:
        return "alpha-lower-double-bracket", match

    match = re.match(r"\((\d+)\)", text)
    if match:
        return "arabic-double-bracket", match

    match = re.match(r"\(((x[cl]|l?x{0,3})(i[xv]|v?i{0,3}))\)", text)
    if match:  # I. II III.
        return "roman-lower-double-bracket", match

    return None, None


def extract_number(text):
    number_type, match = extract_number_match(text)
    if not number_type:
        return None, None
    return number_type, match[1]


split_double_units_pattern_unit_str = (
    # fmt: off
    r'\((?:'
        r"[A-Za-z]"
    r'|'
        r"\d+"
    r'|'
        r"(?:x[cl]|l?x{0,3})(?:i[xv]|v?i{0,3})"
    r')\)'
    # fmt: on
)

split_double_units_pattern = regex.compile(
    # fmt: off
    split_double_units_pattern_unit_str + (
        r'(?:'
            r'[\sA-Za-z\-]{1,50}(?:\.\s|—)'
        r')?'
    )
    # fmt: on
)

split_double_units_same_units_pattern = regex.compile(
    r"(" + split_double_units_pattern_unit_str + r"\s?)+(?=\()"
)


def get_length_of_identical_start(str1, str2):
    i = 0
    for i in range(min(len(str1), len(str2))):
        if str1[i] != str2[i]:
            break
    return i


def get_identical_start(str1, str2):
    return str1[: get_length_of_identical_start(str1, str2)]


def split_double_units(texts):
    prev_text = None
    for text in texts:
        # Skip duplicate starts
        if prev_text:
            identical_start = get_identical_start(text, prev_text)
            prev_text = text
            if len(identical_start) >= 3:
                match = split_double_units_same_units_pattern.match(identical_start)
                if match:
                    text = text[len(match[0]) :]
        else:
            prev_text = text

        # Generate list of texts
        match = True
        while match:
            split_pos = 0 if match is True else match.end()
            prev_text = text[:split_pos]
            text = text[split_pos:]
            match = split_double_units_pattern.match(text)
            if match:
                if prev_text:  # Skip on first pass
                    yield prev_text.strip()
            else:
                yield prev_text + text


def get_index_for_value(items, item, default=None):
    try:
        return items.index(item)
    except ValueError:
        return default


roman_lower_list = [
    "i",
    "ii",
    "iii",
    "iv",
    "v",
    "vi",
    "vi",
    "vii",
    "vii",
    "ix",
    "x",
    "xi",
    "xii",
    "xiii",
    "xiv",
    "xv",
    "xvi",
    "xvii",
    "xviii",
    "xix",
    "xx",
    "xxi",
    "xxii",
    "xxiii",
    "xxiv",
    "xxv",
    "xxvi",
    "xxvii",
    "xxviii",
    "xxix",
    "xxx",
    "xxxi",
    "xxxii",
    "xxxiii",
    "xxxiv",
] + ["viv", "vv", "vvi"]

alpha_lower_list = (
    list(string.ascii_lowercase)
    + [c * 2 for c in string.ascii_lowercase]
    + [c * 3 for c in string.ascii_lowercase]
)


def distance_to_occurence(search_list, start_pos, value, before=False):
    if before:
        sublist = list(reversed(search_list[:start_pos]))
    else:  # after
        sublist = search_list[start_pos + 1 :]
    res = get_index_for_value(sublist, value, default=False)
    if type(res) is int:
        return res + 1


def disambiguate_number_types(number_types):
    values = [num for _, num in number_types]
    types = [t for t, _ in number_types]

    for i, number_type in list(enumerate(number_types)):
        number_type_tag, number = number_type
        if number_type_tag != (
            "alpha-lower-double-bracket",
            "roman-lower-double-bracket",
        ):
            continue

        # Resolve simple cases, only one type in list
        roman_in_list = "roman-lower-double-bracket" in types
        alpha_in_list = "alpha-lower-double-bracket" in types
        unclear_in_list = (
            "alpha-lower-double-bracket",
            "roman-lower-double-bracket",
        ) in types[:i] + types[i + 1 :]
        if roman_in_list and not alpha_in_list and not unclear_in_list:
            number_types[i] = ("roman-lower-double-bracket", number)
            continue
        elif alpha_in_list and not roman_in_list and not unclear_in_list:
            number_types[i] = ("alpha-lower-double-bracket", number)
            continue

        alpha_lower_list_pos = alpha_lower_list.index(number)
        roman_lower_list_pos = roman_lower_list.index(number)

        alpha_prev = None
        roman_prev = None
        if i > 0:  # not first element
            if alpha_lower_list_pos:
                alpha_prev = distance_to_occurence(
                    values, i, alpha_lower_list[alpha_lower_list_pos - 1], before=True
                )
            if roman_lower_list_pos:
                roman_prev = distance_to_occurence(
                    values, i, roman_lower_list[roman_lower_list_pos - 1], before=True
                )

        alpha_post = None
        roman_post = None
        if i + 1 < len(number_types):  # not last element
            if alpha_lower_list_pos + 1 < len(alpha_lower_list):
                alpha_post = distance_to_occurence(
                    values, i, alpha_lower_list[alpha_lower_list_pos + 1]
                )
            if roman_lower_list_pos + 1 < len(roman_lower_list):
                roman_post = distance_to_occurence(
                    values, i, roman_lower_list[roman_lower_list_pos + 1]
                )

        if (alpha_prev == 1 or alpha_post == 1) and roman_prev != 1 and roman_post != 1:
            number_types[i] = ("alpha-lower-double-bracket", number)
            continue
        elif (
            (roman_prev == 1 or roman_post == 1) and alpha_prev != 1 and alpha_post != 1
        ):
            number_types[i] = ("roman-lower-double-bracket", number)
            continue

        # Decide by closest occurrence that follows the sequence
        lowest_dist_list = list(
            filter(lambda x: bool(x), [alpha_prev, roman_prev, alpha_post, roman_post])
        )
        if lowest_dist_list:
            lowest_dist = min(lowest_dist_list)

            if lowest_dist == alpha_prev:
                number_types[i] = ("alpha-lower-double-bracket", number)
                continue
            elif lowest_dist == roman_prev:
                number_types[i] = ("roman-lower-double-bracket", number)
                continue
            elif lowest_dist == alpha_post:
                number_types[i] = ("alpha-lower-double-bracket", number)
                continue
            elif lowest_dist == roman_post:
                number_types[i] = ("roman-lower-double-bracket", number)
                continue
        else:
            if alpha_lower_list_pos == 0:
                number_types[i] = ("alpha-lower-double-bracket", number)
                continue
            elif roman_lower_list_pos == 0:
                number_types[i] = ("roman-lower-double-bracket", number)
                continue

        # print(i, "-", values)
        number_types[i] = ("roman-lower-double-bracket", number)
        continue

    assert not any(type(t) is tuple for t, v in number_types), number_types


def levels_of_number_types(number_types):
    number_levels = []
    for number_type, val in number_types:
        if number_type not in number_levels:
            number_levels.append(number_type)

    # Set texts without numbering at highest level
    number_levels = [None] + [n for n in number_levels if n]
    return number_levels


def parse_cfr_subseqitems(texts, law_key, seqitem_level):
    texts = list(split_double_units(texts))

    if len(texts) == 1:
        text_element = lxml.etree.Element("text")
        text_element.text = texts[0]
        return [text_element]
    else:
        number_types = [extract_number(t) for t in texts]
        disambiguate_number_types(number_types)
        number_levels = levels_of_number_types(number_types)

        result = []
        cursor = []

        for (number_type, val), text in zip(number_types, texts):
            subseqitem_element = lxml.etree.Element("subseqitem")
            text_element = lxml.etree.Element("text")
            text_element.text = text
            subseqitem_element.append(text_element)

            level = number_levels.index(number_type)

            # Cursor rollup so it applies to the current element
            cursor = [
                (elem, elem_level) for elem, elem_level in cursor if elem_level < level
            ]
            subseqitem_element.attrib["level"] = str(seqitem_level + len(cursor) + 1)

            # Add new element to tree
            if cursor:
                cursor[-1][0].append(subseqitem_element)
            else:
                result.append(subseqitem_element)

            # Update cursor
            cursor.append((subseqitem_element, level))

        return result


def parse_cfr_section(section_element, law_key, level):
    """
    Parse a section element.
    :param section_element:
    :return:
    """

    if level == 0:
        raise RuntimeError(
            "parse_cfr_section called with level < 1: {0}".format(section_element)
        )
    output_container = lxml.etree.Element("seqitem")
    output_container.attrib["level"] = str(level)

    section_number = ""
    section_subject = ""
    key_suffix = ""
    texts = []
    for child_element in section_element.getchildren():
        if child_element.tag in ("P", "FP"):
            # process child elements
            texts.append(extract_text(child_element).strip())
            # debug_list.append(lxml.etree.tostring(
            #     child_element, encoding="utf-8", method='text').strip())
        elif child_element.tag == "SECTNO":
            section_number = child_element.text
            # TODO: Discuss
            if section_number:
                key_suffix = "".join(
                    [
                        c
                        for c in section_number
                        if c in string.ascii_letters or c in string.digits or c in "."
                    ]
                )
        elif child_element.tag == "SUBJECT":
            section_subject = child_element.text
        elif child_element.tag in [
            "PRTPAGE",
            "CITA",
            "FTNT",
            "GPH",
            "EDNOTE",
            "RESERVED",
            "NOTE",
            "EFFDNOT",
            "EAR",
            "APPRO",
            "STARS",
            "MATH",
            "LRH",
            "RRH",
            "CONTENTS",
            "SUBTITLE",
            "CTRHD",
            "WEDNOTE",
            "CROSSREF",
            "WEFFDNO",
            "TEFFDNO",
            "EXT-XREF",
            "EFFDNOTP",
            "PARAUTH",
            "WEFFDNOP",
            "EXHIBIT",
            "SECAUTH",
            "AUTH",
            "LHD1",
        ]:
            pass
        elif child_element.tag in (
            "EXTRACT",
            "GPOTABLE",
            "HD",
            "LDRWK",
            "WIDE",
            "EXAMPLE",
            "BOXTXT",
            "TEAR",
            "WBOXTXT",
            "TSECT",
        ):
            new_text = extract_text(child_element).strip()
            if texts:
                texts[-1] = texts[-1] + " " + new_text
            else:
                texts.append(new_text)
        else:
            raise Exception(
                child_element.tag + "------" + str(lxml.etree.tostring(section_element))
            )

    output_container.attrib["heading"] = "{0} {1}".format(
        section_number, section_subject
    ).strip()
    assert "_" not in law_key.split("v")[0], law_key
    assert "_" not in key_suffix, key_suffix
    output_container.attrib["citekey"] = law_key.split("v")[0] + "_" + key_suffix

    output_container.extend(parse_cfr_subseqitems(texts, law_key, seqitem_level=level))

    return output_container


def document_element_attribs():
    xsi_url = "http://www.w3.org/2001/XMLSchema-instance"
    lxml.etree.register_namespace("xsi", xsi_url)
    noNamespaceSchemaLocation = lxml.etree.QName(xsi_url, "noNamespaceSchemaLocation")
    return {
        "level": str(0),
        noNamespaceSchemaLocation: "../../xml-schema.xsd",
        "document_type": "regulation",
    }


allowed_prev_tags = ["TOC", "LRH", "RRH"]


def get_heading_text(
    container_element, consider_previous_chapter=True, first_chapter_in_vol=True
):
    child_element = container_element.find("./HD")
    if child_element is not None:
        return child_element.text and child_element.text.strip()

    child_elements = container_element.xpath("./TOC/TOCHD/HD")
    if child_elements:
        return " ".join([t.text.strip() for t in child_elements])

    reserved_element = container_element.find("./RESERVED")
    if reserved_element is not None:
        return reserved_element.text and reserved_element.text.strip()

    if container_element.tag == "CHAPTER":
        if consider_previous_chapter:
            prev = None
            prevs = container_element.getparent().getparent().xpath(".//CHAPTER")
            if container_element in prevs and prevs.index(container_element):
                idx = prevs.index(container_element)
                prev = prevs[idx - 1]

            if prev is not None:
                prev_tags = [t.tag for t in prev.getchildren()]
                if "TOC" in prev_tags and all(
                    t in allowed_prev_tags for t in prev_tags
                ):
                    return get_heading_text(prev, consider_previous_chapter=False)

        if first_chapter_in_vol:
            chaptis = container_element.xpath(
                "/CFRDOC/TOC/TITLENO/CHAPTI/SUBJECT"
            ) or container_element.xpath("/CFRDOC/FMTR/TOC/TITLENO/CHAPTI/SUBJECT")
            for chapti in chaptis:
                if chapti.text and chapti.text.strip().lower().startswith("chap"):
                    return chapti.text.strip()

    return None


def parse_cfr_container(
    container_element, law_key, level=0, first_chapter_in_vol=False
):
    """
    Parse a container element.
    :param container_element:
    :return:
    """

    # setup element with appropriate level-based tag name
    if level == 0:
        output_container = lxml.etree.Element(
            "document",
            attrib=document_element_attribs(),
        )
    else:
        output_container = lxml.etree.Element("item", attrib={"level": str(level)})

    # get heading first so that we can pass the key downwards in the tree
    heading_text = get_heading_text(
        container_element,
        consider_previous_chapter=True,
        first_chapter_in_vol=first_chapter_in_vol,
    )

    output_container.attrib["heading"] = heading_text or container_element.tag

    # Update first_chapter_in_vol that is used in get_heading_text
    if first_chapter_in_vol and container_element.tag == "CHAPTER":
        first_chapter_in_vol = False

    auth_element = container_element.find("./AUTH/P")
    if auth_element is not None:
        auth_text = lxml.etree.tostring(auth_element, encoding="utf-8", method="text")
        if auth_text:
            output_container.attrib["auth_text"] = auth_text.strip().decode("UTF-8")

    # iterate through children and process recursively
    for child_element in container_element.getchildren():
        if [c.tag for c in child_element.getchildren()] == ["TOC"]:
            pass
        elif child_element.tag in CONTAINER_TAG_SET:
            # recursively process containers
            elems, first_chapter_in_vol = parse_cfr_container(
                child_element, law_key, level + 1, first_chapter_in_vol
            )
            output_container.append(elems)
        elif child_element.tag == "SECTION":
            # process "child" sections
            output_container.append(
                parse_cfr_section(child_element, law_key, level + 1)
            )

    return output_container, first_chapter_in_vol


def get_law_key(title_number, volume_number, file_year):
    return "cfr{0}v{1}_{2}".format(title_number, volume_number, file_year)


def parse_cfr_xml_file(xml_file):
    """
    Parse a CFR XML file.
    :param xml_file:
    :return:
    """
    name_tokens = xml_file.name.split("/")[1].split("-")
    file_year = name_tokens[1]
    title_number = name_tokens[2][5:]
    volume_number = name_tokens[3][3 : -len(".xml")]

    xml_doc = lxml.etree.parse(xml_file)

    title_output_elements = []

    first_chapter_in_vol = True

    for title_element in xml_doc.xpath("/CFRDOC/TITLE"):
        law_key = get_law_key(title_number, volume_number, file_year)
        title_output_element, first_chapter_in_vol = parse_cfr_container(
            title_element, law_key, first_chapter_in_vol=first_chapter_in_vol
        )
        title_output_element.attrib["year"] = file_year
        title_output_element.attrib["title"] = title_number
        title_output_element.attrib["volume"] = volume_number

        title_output_elements.append(title_output_element)

    if len(title_output_elements) > 1:
        queued_title_output_elements = deque(title_output_elements)
        title_output_elements = []
        while queued_title_output_elements:
            title_output_element = queued_title_output_elements.popleft()
            # Not last element
            if len(queued_title_output_elements):

                # contains only one element
                item_count = len(title_output_element.xpath("//item | //seqitem"))
                if item_count == 1:
                    item_elem = title_output_element.xpath("./item")[0]

                    # Item has a heading
                    if "heading" in item_elem.attrib:

                        # Next elem has only one direct child
                        next_items = queued_title_output_elements[0].xpath("./item")
                        if len(next_items) == 1:
                            next_item = next_items[0]

                            # This direct child has no heading
                            if "heading" not in next_item.attrib:

                                # Add content of next elem to current and deque
                                # next elem
                                item_elem.extend(next_item.getchildren())
                                queued_title_output_elements.popleft()

            title_output_elements.append(title_output_element)

    for title_output_element in title_output_elements:
        for element in title_output_element.xpath("//item | //seqitem | //subseqitem"):
            element.attrib["key"] = law_key

    return title_output_elements


def parse_cfr_zip(file_name):
    """
    Parse a CFR zip.
    :param file_name:
    :return:
    """
    with zipfile.ZipFile(file_name) as zip_file:
        complete_title_element = lxml.etree.Element(
            "document", attrib=document_element_attribs()
        )
        last_title_number = None
        for member_info in sorted(zip_file.namelist()):
            name_tokens = member_info.split("/")[1].split("-")
            file_year = name_tokens[1]
            title_number = name_tokens[2][5:]

            with zip_file.open(member_info) as member_file:
                for file_output_element in parse_cfr_xml_file(member_file):
                    current_title_number = file_output_element.attrib["title"]
                    if last_title_number is None:
                        last_title_number = current_title_number
                    elif current_title_number != last_title_number:
                        # output last title when complete
                        finish_title(
                            complete_title_element, file_year, last_title_number
                        )

                        # reset
                        complete_title_element = lxml.etree.Element(
                            "document", attrib=document_element_attribs()
                        )
                        last_title_number = title_number

                    # extend current title
                    complete_title_element.extend(file_output_element.getchildren())

        # output final title
        if len(complete_title_element.getchildren()) > 0:
            # output last title when complete
            finish_title(complete_title_element, file_year, last_title_number)


def finish_title(complete_title_element, file_year, last_title_number):
    complete_title_element.attrib["year"] = file_year
    complete_title_element.attrib["title"] = last_title_number
    complete_title_element.attrib["heading"] = f"Title {last_title_number}"
    complete_title_element.attrib[
        "key"
    ] = f"{get_law_key(last_title_number, 0, file_year)}_{1:06d}"

    counters = defaultdict(int)

    merge_continued_items(complete_title_element)

    for element in complete_title_element.xpath("//item | //seqitem | //subseqitem"):
        vol_key = element.attrib["key"]
        counters[vol_key] += 1
        element.attrib["key"] = f"{vol_key}_{counters[vol_key]:06d}"

    output_file_name = "cfr{0}_{1}.xml".format(last_title_number, file_year)
    output_file_path = os.path.join(US_REG_XML_PATH, output_file_name)
    with open(output_file_path, "wb") as output_file:
        output_xml_doc = lxml.etree.ElementTree(complete_title_element)
        output_file.write(
            lxml.etree.tostring(
                output_xml_doc,
                encoding="utf-8",
                doctype='<?xml version="1.0" encoding="utf-8"?>'
                '<?xml-stylesheet href="../../xml-styles.css"?>',
                pretty_print=True,
            )
        )


def merge_continued_items(complete_title_element):
    def shorten_heading(tag):
        return " ".join(tag.attrib.get("heading", "").split(" ")[:2]).lower().strip()

    prev_items = {}

    last_key_prefix = None

    for item in complete_title_element.xpath("//item"):

        if last_key_prefix != item.attrib["key"]:
            beginning_of_volume = True
            last_key_prefix = item.attrib["key"]

        if beginning_of_volume:
            continued_item = prev_items.get(
                (item.attrib["level"], shorten_heading(item))
            )
        else:
            continued_item = None

        if continued_item is not None:
            continued_item.extend(item.getchildren())
            item.getparent().remove(item)
        else:
            beginning_of_volume = False
            heading = shorten_heading(item)
            if heading:
                prev_items[(item.attrib["level"], heading)] = item
