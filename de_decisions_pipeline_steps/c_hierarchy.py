import multiprocessing
import os
import re

from bs4 import BeautifulSoup

from de_decisions_pipeline_steps.common import get_docparts_with_p
from statics import DE_DECISIONS_HIERARCHY, DE_DECISIONS_XML
from utils.common import ensure_exists, list_dir


def extract_number(text, token_position=0):
    if len(text) == 0 or len(text.split()) <= token_position:
        return None, None
    first_token = text.split()[token_position]

    match = re.fullmatch(r"(([a-h])\2*)\)", first_token)
    if match:  # a) aa) aaa)
        level = len(match[1])
        return f"alpha-lower-bracket-{level}", match[1]

    match = re.fullmatch(r"\((([a-h])\2*)\)", first_token)
    if match:  # (a) (aa) (aaa)
        level = len(match[1])
        return f"alpha-lower-double-bracket-{level}", match[1]

    match = re.fullmatch(r"(([a-h])\2*)\.", first_token)
    if match:  # a. aa. aaa.
        level = len(match[1])
        return f"alpha-lower-dot-{level}", match[1]

    match = re.fullmatch(r"((X[CL]|L?X{0,3})(I[XV]|V?I{0,3}))\.?", first_token)
    if match:  # I. II III.
        return "roman-upper", match[1]

    match = re.fullmatch(r"(\d+)\.", first_token)
    if match:  # 1. 2. 3.
        return "arabic-dot", match[1]

    match = re.fullmatch(r"(\d+)\)", first_token)
    if match:
        return "arabic-bracket", match[1]

    match = re.fullmatch(
        r"([A-H])\.", first_token
    )  # Only until "H." Others are mostly false-positives and disambiguation would be required.
    if match:
        return "alpha-dot", match[1]

    match = re.fullmatch(r"([A-H])\)", first_token)
    if match:
        return "alpha-bracket", match[1]

    match = re.fullmatch(r"(\d(\.\d)*)\.?", first_token)
    if match and len(match[0]) > 1:
        level = len(match[1].split("."))
        return f"numeric-{level}", match[1]

    return None, None


master_order = [
    "alpha-dot",
    "alpha-bracket",
    "roman-upper",
    "arabic-dot",
    "arabic-bracket",
    "numeric-2",
    "numeric-3",
    "numeric-4",
    "numeric-5",
    "alpha-lower-dot-1",
    "alpha-lower-bracket-1",
    "alpha-lower-dot-2",
    "alpha-lower-bracket-2",
    "alpha-lower-dot-3",
    "alpha-lower-bracket-3",
    "alpha-lower-bracket-4",
    "alpha-lower-double-bracket-1",
    "alpha-lower-double-bracket-2",
    "alpha-lower-double-bracket-3",
    "alpha-lower-double-bracket-4",
]


def extract_hierarchy(decision):
    if not os.path.exists(f"{DE_DECISIONS_HIERARCHY}/{decision}"):
        with open(f"{DE_DECISIONS_XML}/{decision}", encoding="utf8") as f:
            soup = BeautifulSoup(f.read(), "lxml-xml")
        for doc_parts in get_docparts_with_p(soup):
            #             has_numbered_ps = bool(doc_parts.find('p', attrs={'numbers': True}))
            for p in doc_parts.find_all("p", {"indented": str(True)}):
                text = p.get_text().strip()
                for token_position in range(3):
                    match_type, value = extract_number(text, token_position)
                    if match_type:
                        if token_position == 0:
                            p.attrs["hierarchy_num_type"] = match_type
                            p.attrs["hierarchy_num"] = value
                        else:
                            if match_type == "alpha-dot":
                                break
                            p.attrs["hierarchy_num_type"] += "," + match_type
                            p.attrs["hierarchy_num"] += "," + value
                    else:
                        break

        #             hierarchy_num_types = list()
        #             for p in doc_parts.find_all('p', attrs={'hierarchy_num_type': True}):
        #                 for hierarchy_num_type in p.attrs['hierarchy_num_type'].split(','):
        #                     if hierarchy_num_type not in hierarchy_num_types:
        #                         hierarchy_num_types.append(hierarchy_num_type)

        #             if hierarchy_num_types:
        #                 unknown_order = len(set(hierarchy_num_types) - set(master_order))
        #                 if not unknown_order:
        #                     hierarchy_num_types_ordered = sorted(hierarchy_num_types, key=lambda x: master_order.index(x))
        #                 if unknown_order or tuple(hierarchy_num_types) != tuple(hierarchy_num_types_ordered):
        #                     print(decision, doc_parts.name, hierarchy_num_types)

        nested_soup = BeautifulSoup("", "lxml-xml")
        assert len(soup.gertyp.get_text()), decision
        assert len(soup.find("entsch-datum").get_text()) == 8, decision
        assert len(soup.aktenzeichen.get_text()), decision
        assert len(soup.doktyp.get_text()), decision

        datum_raw = soup.find("entsch-datum").get_text()
        datum = f"{datum_raw[:4]}-{datum_raw[4:6]}-{datum_raw[6:]}"

        nested_soup.append(
            nested_soup.new_tag(
                "document",
                gericht=soup.gertyp.get_text(),
                datum=datum,
                az=soup.aktenzeichen.get_text(),
                doktyp=soup.doktyp.get_text(),
            )
        )

        if len(soup.spruchkoerper.get_text()):
            nested_soup.document.attrs["spruchkoerper"] = soup.spruchkoerper.get_text()

        if len(soup.norm.get_text()):
            nested_soup.document.append(nested_soup.new_tag("norm"))
            nested_soup.norm.append(nested_soup.new_string(soup.norm.get_text(" ")))

        for doc_part in [
            soup.tenor,
            soup.tatbestand,
            soup.entscheidungsgruende,
            soup.gruende,
            soup.abwmeinung,
            soup.sonstlt,
        ]:
            if not len(doc_part.get_text()):
                continue
            item = nested_soup.new_tag("item", heading=doc_part.name)
            nested_soup.document.append(item)

            open_tags = [dict(tag=item, level=-1)]
            text_tag = None
            for p in doc_part.find_all("p"):
                if text_tag and "indented" in p.attrs:
                    assert p.attrs["indented"] == "True"
                    text_tag.append(" " + nested_soup.new_string(p.get_text(" ")))
                    continue

                if "hierarchy_num_type" in p.attrs:
                    for num_type, num in zip(
                        p.attrs["hierarchy_num_type"].split(","),
                        p.attrs["hierarchy_num"].split(","),
                    ):
                        current_level = master_order.index(num_type)
                        while open_tags[-1]["level"] >= current_level:
                            open_tags.pop()

                        item = nested_soup.new_tag("item", heading=num)
                        open_tags[-1]["tag"].append(item)
                        open_tags.append(dict(tag=item, level=current_level))

                text_tag = nested_soup.new_tag("text")
                text_tag.append(nested_soup.new_string(p.get_text(" ")))
                seqitem = nested_soup.new_tag("seqitem")
                seqitem.append(text_tag)
                open_tags[-1]["tag"].append(seqitem)

        decision_id = decision.split(".")[0]
        nodeid_counter = 0
        for tag in nested_soup.find_all(["document", "item", "seqitem"]):
            tag.attrs["key"] = f"{decision_id}_{nodeid_counter:06d}"
            nodeid_counter += 1
            tag.attrs["level"] = (
                0 if tag.name == "document" else tag.parent.attrs["level"] + 1
            )

        with open(f"{DE_DECISIONS_HIERARCHY}/{decision}", "w", encoding="utf8") as f:
            f.write(str(nested_soup))


def hierarchy():
    ensure_exists(DE_DECISIONS_HIERARCHY)
    decisions = list_dir(DE_DECISIONS_XML, ".xml")

    with multiprocessing.Pool() as p:
        p.map(extract_hierarchy, decisions)
