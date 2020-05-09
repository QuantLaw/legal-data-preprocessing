import os
import re

from bs4 import BeautifulSoup

from utils.common import ensure_exists, list_dir
from statics import DE_XML_PATH, DE_ORIGINAL_PATH


def de_to_xml_prepare(overwrite):
    ensure_exists(DE_XML_PATH)
    files = list_dir(DE_ORIGINAL_PATH, ".xml")

    if not overwrite:
        existing_files = list_dir(DE_XML_PATH, ".xml")

        # Remove cite_key
        converted_existing_files = [
            f.split("_")[0] + "_" + "_".join(f.split("_")[2:]) for f in existing_files
        ]
        files = list(filter(lambda f: f not in converted_existing_files, files))

    return sorted(files)


def de_to_xml(filename):
    with open(f"{DE_ORIGINAL_PATH}/{filename}") as f:
        soup = BeautifulSoup(f.read(), "lxml-xml")
    convert_to_xml(soup, filename)


#######################################
# Functions
#######################################


def create_root_elment(s_rahmen, t_soup):
    t_document = t_soup.new_tag(
        "document",
        attrs={
            "level": 0,
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:noNamespaceSchemaLocation": "../../pipeline/xml-schema.xsd",
        },
    )
    heading = s_rahmen.langue
    heading = heading and heading.string.strip()
    if heading:
        t_document.attrs["heading"] = heading

    s_kurzue = s_rahmen.kurzue
    s_kurzue = s_kurzue and s_kurzue.string.strip()
    if s_kurzue:
        t_document.attrs["heading_short"] = s_kurzue

    s_jurabk = s_rahmen.jurabk
    s_jurabk = s_jurabk and s_jurabk.string.strip()
    assert s_jurabk
    if s_jurabk:
        t_document.attrs["abk_1"] = s_jurabk

    s_amtabk = s_rahmen.amtabk
    s_amtabk = s_amtabk and s_amtabk.string.strip()
    if s_amtabk:
        t_document.attrs["abk_2"] = s_amtabk
    t_soup.append(t_document)

    return t_document, s_jurabk


analyse_is_preamble_pattern = re.compile(
    r"§(.*?)|Art(ikel)?\.?\s+([0-9]|\b(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})\b)(.*?)",
    flags=re.IGNORECASE,
)


def analyse_is_preamble(s_metadaten, s_enbez):
    if s_metadaten.find("gliederungseinheit", recursive=False):
        return False

    if s_enbez and analyse_is_preamble_pattern.match(s_enbez):
        return False

    return True


analyse_is_appendix_pattern = re.compile(
    r"(Anlagen?|Anhang|Schlu.s?formel|Tabelle \w+ zu)\b", flags=re.IGNORECASE,
)

remove_removed_items_pattern = re.compile(
    r".{0,5}(weggefallen|\-|\—|aufgehoben|entf.ll(t|en)|gestrichen|unbesetzt).{0,5}",
    flags=re.IGNORECASE,
)


def nodeid_counter():
    nodeid_counter.counter += 1
    return nodeid_counter.counter


citekey_enbez_pattern = re.compile(r"(§§?|Art[a-z\.]*)\s?(\d+[a-z]*)\b")


def convert_to_xml(source_soup, filename):
    t_soup = BeautifulSoup("", "lxml-xml")

    s_norms = source_soup.dokumente.find_all("norm", recursive=False)

    if not len(s_norms):
        print("EMPTY FILE", filename)
        return

    s_rahmen = s_norms[0]
    s_norms = s_norms[1:]

    t_document, jurabk = create_root_elment(s_rahmen, t_soup)

    citekey_prefix = re.sub(r"[^\wäöüÄÖÜß]", "-", jurabk)

    cursor = [t_document]

    t_items = []

    is_preamble = True
    is_appendix = False
    last_gliederungskennzahl = None
    for s_norm in s_norms:
        s_metadaten = s_norm.find("metadaten", recursive=False)
        s_textdaten = s_norm.find("textdaten", recursive=False)
        s_enbez = s_metadaten.find("enbez", recursive=False)
        s_enbez = s_enbez and s_enbez.string.strip()
        s_gliederungseinheit = s_metadaten.find("gliederungseinheit", recursive=False)
        s_text = s_textdaten and s_textdaten.find("text", recursive=False)

        # Skip if preamble or appendix
        if is_preamble:
            is_preamble = analyse_is_preamble(s_metadaten, s_enbez)
        if s_enbez and not is_appendix:
            is_appendix = bool(analyse_is_appendix_pattern.match(s_enbez))
        if is_preamble or is_appendix:
            continue

        if s_gliederungseinheit:
            # is Item
            s_gliederungskennzahl = s_gliederungseinheit.find(
                "gliederungskennzahl", recursive=False
            ).string
            level_3 = len(s_gliederungskennzahl)
            assert level_3 % 3 == 0
            level = int(level_3 / 3)
            assert level > 0

            if last_gliederungskennzahl != s_gliederungskennzahl:
                last_gliederungskennzahl = s_gliederungskennzahl

                s_gliederungsbez = s_gliederungseinheit.find(
                    "gliederungsbez", recursive=False
                ).string

                s_gliederungstitel = s_gliederungseinheit.find(
                    "gliederungstitel", recursive=False
                )
                s_gliederungstitel = s_gliederungstitel and s_gliederungstitel.string

                heading = (
                    f"{s_gliederungsbez} {s_gliederungstitel}"
                    if s_gliederungstitel
                    else s_gliederungsbez
                )
                heading = heading.strip()

                corrected_level = min(level, len(cursor))
                parent = cursor[corrected_level - 1]

                t_item = t_soup.new_tag(
                    "item", attrs={"level": corrected_level, "heading": heading}
                )

                parent.append(t_item)
                t_items.append(t_item)
                cursor = cursor[:corrected_level] + [t_item]

        if s_enbez and s_enbez.lower() in [
            "inhaltsverzeichnis",
            "eingangsformel",
            "inhaltsübersicht",
            "inhalt",
        ]:
            continue

        if s_text:  # is seqitem
            t_seqitem = t_soup.new_tag("seqitem", attrs={"level": len(cursor)})
            if s_enbez:
                t_seqitem.attrs["heading"] = s_enbez
            s_content = s_text.find("Content", recursive=False)

            texts = []
            if s_content:
                for s_p in s_content.find_all("P", recursive=False):
                    text = s_p.string
                    if text and (
                        len(text) > 14
                        or not remove_removed_items_pattern.fullmatch(text)
                    ):
                        texts.append(text)

            if len(texts) == 1:

                t_text = t_soup.new_tag("text")
                t_text.append(t_soup.new_string(texts[0]))
                t_seqitem.append(t_text)
            elif len(texts) > 1:
                for text in texts:
                    t_subseqitem = t_soup.new_tag(
                        "subseqitem", attrs={"level": len(cursor) + 1}
                    )
                    t_text = t_soup.new_tag("text")
                    t_subseqitem.append(t_text)
                    t_text.append(t_soup.new_string(text))
                    t_seqitem.append(t_subseqitem)

            if texts:
                cursor[-1].append(t_seqitem)

    # iterate over soup in reverse item order to ensure empty item removal works
    for t in reversed(t_items):
        if t.name == "item":
            if "weggefallen" in t.attrs.get("heading").lower():
                t.decompose()
            # items without content should go (assuming some variant of "weggefallen")
            elif len(t.contents) == 0:
                t.decompose()

    doknr, start_date, end_date = os.path.splitext(filename)[0].split("_")
    target_filename = (
        f"{DE_XML_PATH}/"
        + "_".join([doknr, citekey_prefix, start_date, end_date])
        + ".xml"
    )

    nodeid_counter.counter = 0
    file_id = "_".join([doknr, citekey_prefix, end_date])
    for t in t_soup.find_all(["document", "item", "seqitem", "subseqitem"]):
        t.attrs["key"] = f"{file_id}_{nodeid_counter():06d}"

        if t.name == "seqitem":
            if "heading" in t.attrs:
                heading = t.attrs["heading"]
                match = citekey_enbez_pattern.match(heading)
                if match:
                    t.attrs["citekey"] = f"{citekey_prefix}_{match[2]}"
                else:
                    print(f"Cannot create citekey of {heading} in {target_filename}")

    with open(target_filename, "w", encoding="utf8") as f:
        f.write(str(t_soup))
