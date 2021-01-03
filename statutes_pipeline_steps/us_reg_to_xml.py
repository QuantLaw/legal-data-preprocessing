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


def item_not_complete(item, existing_files):
    with zipfile.ZipFile(os.path.join(US_REG_INPUT_PATH, item)) as zip_file:
        required_titles = {
            f"{file.split('/')[0].split('-')[-1]}" for file in zip_file.namelist()
        }
    year = os.path.splitext(item)[0]

    required_files = {"cfr{0}_{1}.xml".format(t, year) for t in required_titles}
    missing_files = required_files - existing_files
    return bool(missing_files)


class UsRegsToXmlStep(PipelineStep):
    def get_items(self, overwrite) -> list:
        # Create target folder
        ensure_exists(US_REG_XML_PATH)

        # Get source files
        files = list_dir(US_REG_INPUT_PATH, ".zip")

        if not overwrite:
            existing_files = set(list_dir(US_REG_XML_PATH, ".xml"))
            files = [f for f in files if item_not_complete(f, existing_files)]

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


def extract_children_text(e):
    res = []
    if e.text and e.text.strip():
        res.append(extract_text_pattern.sub(" ", e.text).strip())
    for child in e.getchildren():
        text = extract_text(child).strip()
        if text:
            res.append(text)
        if child.tail and child.tail.strip():
            res.append(extract_text_pattern.sub(" ", child.tail).strip())

    return res


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
    if type(text) is list:
        return None, None
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
    r"(" + split_double_units_pattern_unit_str + r"\s?)+(?=\(|$|\s+)"
)


def get_length_of_identical_start(str1, str2):
    i = 0
    for i in range(min(len(str1), len(str2))):
        if str1[i] != str2[i]:
            break
    return i


def get_identical_start(str1, str2):
    return str1[: get_length_of_identical_start(str1, str2) + 1]


def split_double_units_text_splitter(text):
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


def split_double_units(text_lists):
    for idx, text_list in enumerate(text_lists):
        text = text_list[0]
        if type(text) is str:
            # Skip duplicate starts
            if idx:
                prev_text_in_list = text_lists[idx - 1][0]
                identical_start = get_identical_start(text, prev_text_in_list)
                if len(identical_start) >= 3:
                    match = split_double_units_same_units_pattern.match(identical_start)
                    if match:
                        text = text[len(match[0]) :]

            # Generate list of texts
            splitted_text = list(split_double_units_text_splitter(text))
            new_text_splits = splitted_text[:-1]
            last_text_split = splitted_text[-1]
            for new_text_split in new_text_splits:
                yield [new_text_split]
            yield [last_text_split] + text_list[1:]
        else:
            yield text_list


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


def section_element_to_texts(section_element):
    texts = []
    for elem in section_element.getchildren():
        if elem.tag in ("P", "HD", "WIDE", "TEAR"):
            texts.append((extract_text(elem).strip(), False))

        elif elem.tag in ("FP", "LDRWK", "BOXTXT"):
            texts.append((extract_text(elem).strip(), True))
        elif elem.tag in [
            "SECTNO",
            "SUBJECT",
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
            pass  # ignore these tags
        elif elem.tag in (
            "EXTRACT",
            "EXAMPLE",
            "GPOTABLE",
            "TSECT",
            "WBOXTXT",
        ):
            child_texts = list(extract_children_text(elem))
            if child_texts:
                texts.append((child_texts, True))
        else:
            raise Exception(
                elem.tag + "------" + str(lxml.etree.tostring(section_element))
            )

    text_lists = []
    for text, continuation in texts:
        if continuation and text_lists:
            text_lists[-1].append(text)
        else:
            text_lists.append([text])

    return text_lists


def creat_subseqitem_with_text_tag(text):
    subseqitem_element = lxml.etree.Element("subseqitem")
    if type(text) is str:
        text_element = lxml.etree.Element("text")
        text_element.text = text
        subseqitem_element.append(text_element)
    elif len(text) == 1:
        text_element = lxml.etree.Element("text")
        text_element.text = text[0]
        subseqitem_element.append(text_element)
    else:
        assert type(text) is list
        for text_child in text:
            subsubseqitem_element = lxml.etree.Element("subseqitem")
            text_element = lxml.etree.Element("text")
            text_element.text = text_child
            subsubseqitem_element.append(text_element)
            subseqitem_element.append(subsubseqitem_element)
    return subseqitem_element


def parse_cfr_subseqitems(section_element):
    # extract the texts
    text_lists = section_element_to_texts(section_element)
    text_lists = list(split_double_units(text_lists))

    if (
        len(text_lists) == 1
        and len(text_lists[0]) == 1
        and type(text_lists[0][0]) is str
    ):
        text_element = lxml.etree.Element("text")
        text_element.text = text_lists[0][0]
        return [text_element]
    else:
        number_types = [extract_number(t[0]) for t in text_lists]
        disambiguate_number_types(number_types)
        number_levels = levels_of_number_types(number_types)

        result = []
        cursor = []

        for (number_type, val), text_list in zip(number_types, text_lists):
            subseqitem_elements = [creat_subseqitem_with_text_tag(t) for t in text_list]

            level = number_levels.index(number_type)

            # Cursor rollup so it applies to the current element
            cursor = [
                (elem, elem_level) for elem, elem_level in cursor if elem_level < level
            ]

            # Add new element to tree
            if cursor:
                cursor[-1][0].extend(subseqitem_elements)
            else:
                result.extend(subseqitem_elements)

            # Update cursor
            cursor.append((subseqitem_elements[-1], level))

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

    SECTNO_tags = section_element.xpath("./SECTNO")
    section_number = (
        SECTNO_tags[0].text.strip() if SECTNO_tags and SECTNO_tags[0].text else ""
    )

    SUBJECT_tags = section_element.xpath("./SUBJECT")
    section_subject = (
        SUBJECT_tags[0].text.strip() if SUBJECT_tags and SUBJECT_tags[0].text else ""
    )

    key_suffix = (
        "".join(
            [
                c
                for c in section_number
                if c in string.ascii_letters or c in string.digits or c in "."
            ]
        )
        if section_number
        else ""
    )

    output_container.attrib["heading"] = "{0} {1}".format(
        section_number, section_subject
    ).strip()
    assert "_" not in law_key.split("v")[0], law_key
    assert "_" not in key_suffix, key_suffix
    output_container.attrib["citekey"] = law_key.split("v")[0] + "_" + key_suffix

    output_container.extend(parse_cfr_subseqitems(section_element))

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
    heading_text = extract_text_pattern.sub(" ", heading_text).strip()

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

        element.attrib["level"] = str(int(element.getparent().attrib["level"]) + 1)

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
