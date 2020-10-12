import os
import re

import lxml.etree

from statics import US_REG_ORIGINAL_PATH

# regex source:
# https://gist.github.com/mlissner/dda7f6677b98b98f54522e271d486781#file-filters-js-L77

re_cfr_section = re.compile(
    r"([0-9]+[a-z]*) C\.?F\.?R\.? ([0-9]+[a-z]*)", re.IGNORECASE
)
re_cfr_container = re.compile(
    r"([0-9]+[a-z]*) C\.?F\.?R\.? ([a-z]+ [0-9a-z\-]+)", re.IGNORECASE
)


def extract_text(e):
    """
    Extract normalized text from an element.
    :param e:
    :return:
    """
    e_text = lxml.etree.tostring(e, encoding="utf-8", method="text")
    return e_text


def get_section_hierarchy(section_element):
    """
    Get the section element hierarchy.
    :param section_element:
    :return:
    """
    hierarchy_data = []
    h_element = section_element
    while h_element is not None:
        if h_element.tag == "SECTION":
            try:
                section_number = extract_text(h_element.xpath("./SECTNO")[0])
            except Exception:
                section_number = None

            try:
                section_subject = extract_text(h_element.xpath("./SUBJECT")[0])
            except Exception:
                section_subject = None

            hierarchy_data.append((section_number, section_subject))
        else:
            try:
                group_heading = extract_text(h_element.xpath("./HD")[0])
            except Exception:
                group_heading = None
            hierarchy_data.append((group_heading,))

        h_element = h_element.getparent()

    return hierarchy_data


def parse_cfr_xml_file(xml_file, file_name):
    """
    Parse a CFR XML file.
    :param xml_file:
    :return:
    """
    xml_doc = lxml.etree.parse(xml_file)

    for title_element in xml_doc.xpath(".//TITLE"):
        assert len(title_element.xpath("../CHAPTER")) <= 1, file_name
        for section_element in title_element.xpath("./CHAPTER//SECTION"):
            pass
            # section_text = bytes()
            # for p_element in section_element.xpath(".//P"):
            #     section_text += extract_text(p_element) + b"\n"
            # print(get_section_hierarchy(section_element))
            # print(re_cfr_container.findall(section_text))


if __name__ == "__main__":
    for file_name in os.listdir("../" + US_REG_ORIGINAL_PATH):
        if file_name.lower().endswith("xml"):
            with open(os.path.join("..", US_REG_ORIGINAL_PATH, file_name)) as f:
                parse_cfr_xml_file(f, file_name)
