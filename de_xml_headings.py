import regex

from common import list_dir, create_soup, heading_types, categorize_heading_regex
from statics import DE_XML_PATH, DE_XML_HEADING_ORDERS_PATH


def de_xml_heading_order_prepare(overwrite):
    orders_dict = dict()

    with open("de_xml_headings_manual.txt", encoding="utf-8") as f:
        for line in f.readlines():
            entries = line.strip().split(",")
            orders_dict[entries[0]] = entries[1:]

    files = list_dir(DE_XML_PATH, ".xml")

    # filter existing entries
    files = [x for x in files if x not in orders_dict]

    return files


def de_xml_heading_order(filename):
    headings = get_heading_types(f"{DE_XML_PATH}/{filename}")
    headings = disambiguate_types(headings)
    headings = remove_duplicates(headings)
    headings = optimize_headings(headings)

    return filename, headings


def de_xml_heading_order_finish(orders):

    orders_dict = dict()
    for file, order in orders:
        assert file not in orders_dict
        orders_dict[file] = order

    with open("de_xml_headings_manual.txt", encoding="utf-8") as f:
        manual_lines = f.readlines()

    with open(DE_XML_HEADING_ORDERS_PATH, "w", encoding="utf-8") as f:
        f.writelines(manual_lines)
        for file in sorted(orders_dict):
            print(",".join([file, *orders_dict[file]]), file=f)


###########
# Functions
###########


def categorize_heading(heading):
    """get type of heading. This function is also in notebook DE-nest-xmls and should be identical."""
    for heading_type in heading_types:
        if regex.match(
            categorize_heading_regex(heading_type), heading, flags=regex.IGNORECASE
        ):
            return heading_type
    return None


def get_heading_types(filepath):
    headings = []
    soup = create_soup(filepath)
    heading_tags = soup.find_all("heading")

    for tag in heading_tags:
        headings.append(tag.get_text(" "))

    headings = [heading.strip().replace("\n", "\\n") for heading in headings]

    # filter law heading
    headings = headings[1:]

    # filter paragraphs
    headings = [
        x for x in headings if not regex.fullmatch(r"(§(.*?)|Art\.?\s*[0-9](.*?))", x)
    ]

    # Categorize headings
    headings = [categorize_heading(x) for x in headings]
    headings = [x for x in headings if x]

    return headings


def remove_duplicates(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]


def replace(seq, old, new):
    return [new if x == old else x for x in seq]


def disambiguate_types(headings):
    ambiguous_type = ("roman-upper-dot", "alpha-upper-dot")
    if ambiguous_type not in headings:
        return headings
    if "roman-upper-dot" in headings and "alpha-upper-dot" not in headings:
        return replace(headings, ambiguous_type, "roman-upper-dot")
    elif "alpha-upper-dot" in headings and "roman-upper-dot" not in headings:
        return replace(headings, ambiguous_type, "alpha-upper-dot")

    for index, heading in enumerate(headings):
        if heading == ambiguous_type:
            for next_heading in heading[index:]:
                if next_heading in ["roman-upper-dot", "alpha-upper-dot"]:
                    headings[index] = next_heading
                    break
            if headings[index] == ambiguous_type:
                headings[index] = "roman-upper-dot"  # Arbitrary choice
    return headings


def optimize_headings(headings):
    if "Titel" in headings and "Untertitel" in headings:
        move_to_index(headings, "Untertitel", headings.index("Titel") + 1)

    if "Abschnitt" in headings and "Unterabschnitt" in headings:
        move_to_index(headings, "Unterabschnitt", headings.index("Abschnitt") + 1)

    if "Kapitel" in headings and "Unterkapitel" in headings:
        move_to_index(headings, "Unterkapitel", headings.index("Kapitel") + 1)

    if "Artikel" in headings and "Unterartikel" in headings:
        move_to_index(headings, "Unterartikel", headings.index("Artikel") + 1)
    return headings


def move_to_index(list_to_edit, object, index):
    list_to_edit.remove(object)
    list_to_edit.insert(index, object)
