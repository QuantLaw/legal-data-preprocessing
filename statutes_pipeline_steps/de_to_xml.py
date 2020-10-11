import os
import re

from bs4 import BeautifulSoup
from quantlaw.utils.files import ensure_exists, list_dir
from quantlaw.utils.pipeline import PipelineStep

from statics import DE_ORIGINAL_PATH, DE_XML_PATH


class DeToXmlStep(PipelineStep):
    def get_items(self, overwrite) -> list:
        ensure_exists(DE_XML_PATH)
        files = list_dir(DE_ORIGINAL_PATH, ".xml")

        if not overwrite:
            existing_files = list_dir(DE_XML_PATH, ".xml")

            # Remove cite_key
            converted_existing_files = [
                f.split("_")[0] + "_" + "_".join(f.split("_")[2:])
                for f in existing_files
            ]
            files = list(filter(lambda f: f not in converted_existing_files, files))

        return sorted(files)

    def execute_item(self, item):
        with open(f"{DE_ORIGINAL_PATH}/{item}") as f:
            soup = BeautifulSoup(f.read(), "lxml-xml")
        convert_to_xml(soup, item)


###########
# Functions
###########


def create_root_elment(s_rahmen, t_soup):

    # Create the root tag for the xml file the represents a law
    t_document = t_soup.new_tag(
        "document",
        attrs={
            "level": 0,
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:noNamespaceSchemaLocation": "../../xml-schema.xsd",
        },
    )

    # Add attributes

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
        t_document.attrs["abbr_1"] = s_jurabk

    s_amtabk = s_rahmen.amtabk
    s_amtabk = s_amtabk and s_amtabk.string.strip()
    if s_amtabk:
        t_document.attrs["abbr_2"] = s_amtabk

    # add the root tag to the target soup
    t_soup.append(t_document)

    return t_document, s_jurabk


analyse_is_preamble_pattern = re.compile(
    r"§(.*?)|Art(ikel)?\.?\s+([0-9]|\b(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})\b)(.*?)",
    flags=re.IGNORECASE,
)


def analyse_is_preamble(s_metadaten, s_enbez):
    """
    Returns: True if the metadata and enbez tag may belong to a preamble of a law
    """
    if s_metadaten.find("gliederungseinheit", recursive=False):
        return False

    if s_enbez and analyse_is_preamble_pattern.match(s_enbez):
        return False

    return True


analyse_is_appendix_pattern = re.compile(
    r"(Anlagen?|Anhang|Schlu.s?formel|Tabelle \w+ zu)\b",
    flags=re.IGNORECASE,
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
    # Create the target soup
    t_soup = BeautifulSoup("", "lxml-xml")

    # Get all source tags that need to be converted
    s_norms = source_soup.dokumente.find_all("norm", recursive=False)

    # Check that this is no empty file
    if not len(s_norms):
        print("EMPTY FILE", filename)
        return

    # Separate tag representing the whole law from the tags that represent its content
    s_rahmen = s_norms[0]
    s_norms = s_norms[1:]

    # Create a root element
    t_document, jurabk = create_root_elment(s_rahmen, t_soup)

    # Generate a citekey prefix and replacing in it uncommon characters with a dash
    citekey_prefix = re.sub(r"[^\wäöüÄÖÜß]", "-", jurabk)

    # Creating a cursor that contains the element that was created last with all it
    # descendants, in other words it path in the tree.
    cursor = [t_document]

    # The length of gliederungskennzahl of the elements in the curos. These value will
    # be used to determine how the elements are nested
    cursor_gliederungskennzahl_lengths = [0]

    last_gliederungskennzahl = None

    # list containing all target items
    t_items = []

    # booleans that manage if currently an element is analyzed that belongs to the
    # preamble or appendix and should be skipped
    is_preamble = True
    is_appendix = False

    for s_norm in s_norms:
        correct_errors_gliederungskennzahl(filename, s_norm)

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
            assert level_3 > 0
            if level_3 not in cursor_gliederungskennzahl_lengths:
                assert cursor_gliederungskennzahl_lengths[-1] < level_3, (
                    filename,
                    cursor_gliederungskennzahl_lengths,
                    level_3,
                    s_norm,
                )
                cursor_gliederungskennzahl_lengths.append(level_3)
            level = cursor_gliederungskennzahl_lengths.index(level_3)

            if last_gliederungskennzahl != s_gliederungskennzahl:
                last_gliederungskennzahl = s_gliederungskennzahl

                # Add a new item to the tree
                t_item, corrected_level = create_new_item(
                    s_gliederungseinheit, t_soup, level, cursor
                )

                t_items.append(t_item)
                cursor = cursor[:corrected_level] + [t_item]
                cursor_gliederungskennzahl_lengths = cursor_gliederungskennzahl_lengths[
                    : corrected_level + 1
                ]

        if s_enbez and s_enbez.lower() in [
            "inhaltsverzeichnis",
            "eingangsformel",
            "inhaltsübersicht",
            "inhalt",
        ]:
            continue

        if s_text:  # is seqitem
            add_new_seqitem(s_enbez, s_text, t_soup, cursor)

    cleanup_removed_items(t_items)

    adapt_seqitem_level_for_article_laws(t_items)

    doknr, start_date, end_date = os.path.splitext(filename)[0].split("_")

    target_filename = (
        f"{DE_XML_PATH}/"
        + "_".join([doknr, citekey_prefix, start_date, end_date])
        + ".xml"
    )

    # Add attrs key and citekey
    add_item_keys(t_soup, doknr, citekey_prefix, end_date)

    with open(target_filename, "w", encoding="utf8") as f:
        f.write(str(t_soup))


def create_new_item(s_gliederungseinheit, t_soup, level, cursor):
    # Preprate attributes
    s_gliederungsbez = s_gliederungseinheit.find(
        "gliederungsbez", recursive=False
    ).string

    s_gliederungstitel = s_gliederungseinheit.find("gliederungstitel", recursive=False)
    s_gliederungstitel = s_gliederungstitel and s_gliederungstitel.string

    heading = (
        f"{s_gliederungsbez} {s_gliederungstitel}"
        if s_gliederungstitel
        else s_gliederungsbez
    )
    heading = heading.strip()

    corrected_level = min(level, len(cursor))

    # Create the new tag
    t_item = t_soup.new_tag(
        "item", attrs={"level": corrected_level, "heading": heading}
    )

    # Add new tag to soup
    parent = cursor[corrected_level - 1]
    parent.append(t_item)

    return t_item, corrected_level


def add_new_seqitem(s_enbez, s_text, t_soup, cursor):
    # Create new tag
    t_seqitem = t_soup.new_tag("seqitem", attrs={"level": len(cursor)})

    # Add attributes
    if s_enbez:
        t_seqitem.attrs["heading"] = s_enbez

    s_content = s_text.find("Content", recursive=False)

    # Get text content
    texts = []
    if s_content:
        for s_p in s_content.find_all("P", recursive=False):
            text = s_p.string
            if text and (
                len(text) > 14 or not remove_removed_items_pattern.fullmatch(text)
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

    # Add seqitem to soup
    if texts:
        cursor[-1].append(t_seqitem)


def cleanup_removed_items(t_items):
    # iterate over soup in reverse item order to ensure empty item removal works
    for t in reversed(t_items):
        if t.name == "item":
            if "weggefallen" in t.attrs.get("heading").lower():
                t.decompose()
            # items without content should go (assuming some variant of "weggefallen")
            elif len(t.contents) == 0:
                t.decompose()


def adapt_seqitem_level_for_article_laws(t_items):
    """
    Optimize Art. Gesetze e.g. BGBEG
    """
    modified_items = []
    for t_item in t_items:
        if (
            t_item.name == "item"
            and "heading" in t_item.attrs
            and t_item not in modified_items
        ):
            match = citekey_enbez_pattern.match(t_item.attrs["heading"])
            if match:
                t_item.name = "seqitem"
                subitems = list(t_item.find_all(["seqitem", "item"]))
                modified_items.append(t_item)
                modified_items.extend(subitems)
                for subitem in subitems:
                    subitem.name = "subseqitem"
                    if (
                        not subitem.attrs.get("heading")
                        and len(subitem.parent.contents) == 1
                    ):
                        for subsubitem in subitem.find_all(level=True):
                            subsubitem.attrs["level"] -= 1
                        subitem.unwrap()


def add_item_keys(t_soup, doknr, citekey_prefix, end_date):
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
                    print(
                        f"Cannot create citekey of {heading} in",
                        doknr,
                        citekey_prefix,
                        end_date,
                    )


def correct_errors_gliederungskennzahl(filename, s_norm):
    if filename.startswith("BJNR002190897_"):
        if (
            s_norm.gliederungskennzahl
            and s_norm.gliederungskennzahl.text == "030040020"
            and (
                (
                    s_norm.find_previous_siblings("norm")[1].gliederungskennzahl
                    and s_norm.find_previous_siblings("norm")[
                        1
                    ].gliederungskennzahl.text
                    == "030040010010"
                )
                or (
                    s_norm.find_previous_siblings("norm")[3].gliederungskennzahl
                    and s_norm.find_previous_siblings("norm")[
                        3
                    ].gliederungskennzahl.text
                    == "030040010010"
                )
            )
        ):
            s_norm.gliederungskennzahl.string.replace_with("030040010020")
        elif (
            s_norm.gliederungskennzahl
            and s_norm.gliederungskennzahl.text == "030040050"
            and s_norm.find_previous_siblings("norm")[1].gliederungskennzahl.text
            == "030040010040"
        ):
            s_norm.gliederungskennzahl.string.replace_with("030040010050")

    elif filename.startswith("BJNR003410960_"):
        if (
            s_norm.gliederungskennzahl
            and len(s_norm.gliederungskennzahl.text) == 3
            and s_norm.parent.find("gliederungskennzahl").text == "010010"
        ):
            s_norm.gliederungskennzahl.string.replace_with(
                "010" + s_norm.gliederungskennzahl.string
            )

    elif filename.startswith("BJNR004050922_"):
        if (
            s_norm.gliederungskennzahl
            and s_norm.gliederungskennzahl.text == "010110020"
        ):
            parent_str = str(s_norm.parent)
            if ">010110010<" not in parent_str:
                assert ">010110010050<" not in parent_str
                s_norm.gliederungskennzahl.string.replace_with("010110010050")
        if (
            s_norm.gliederungskennzahl
            and s_norm.gliederungskennzahl.text == "010110030"
        ):
            parent_str = str(s_norm.parent)
            if ">010110010<" not in parent_str:
                assert ">010110010060<" not in parent_str
                s_norm.gliederungskennzahl.string.replace_with("010110010060")

    elif filename.startswith("BJNR006049896_"):
        if (
            s_norm.gliederungskennzahl
            and (s_norm.gliederungskennzahl.text[:5] in ["06023", "07023", "07024"])
            and len(s_norm.gliederungskennzahl.text) == 6
        ):
            s_norm.gliederungskennzahl.string.replace_with(
                s_norm.gliederungskennzahl.text[:3]
                + "00"
                + s_norm.gliederungskennzahl.text[3:]
                + "0"
            )

        if s_norm.gliederungskennzahl and s_norm.gliederungskennzahl.text == "050224":
            s_norm.gliederungskennzahl.string.replace_with("050002240")

        if (
            s_norm.gliederungskennzahl
            and s_norm.gliederungskennzahl.text == "010020060470"
            and len(s_norm.find_previous_siblings("norm")[1].gliederungskennzahl.text)
            == len("010020060000480")
        ):
            s_norm.gliederungskennzahl.string.replace_with("010020060000470")

    elif filename.startswith("BJNR008930971_"):
        if (
            s_norm.gliederungskennzahl
            and len(s_norm.gliederungskennzahl.text) == 3
            and s_norm.parent.find("gliederungskennzahl").text == "020020"
        ):
            s_norm.gliederungskennzahl.string.replace_with(
                s_norm.gliederungskennzahl.string * 2
            )

    elif filename.startswith("BJNR059500997_"):
        if (
            s_norm.gliederungskennzahl
            and s_norm.gliederungskennzahl.text.startswith("050020")
            and ">050010<" not in str(s_norm.parent)
        ):
            s_norm.gliederungskennzahl.string.replace_with(
                s_norm.gliederungskennzahl.text[:6]
                + "000"
                + s_norm.gliederungskennzahl.text[6:]
            )

    elif filename.startswith("BJNR101409994_"):
        if (
            s_norm.gliederungskennzahl
            and s_norm.parent.find("gliederungskennzahl").text == "040430"
            and s_norm.gliederungskennzahl.string >= "050"
        ):
            s_norm.gliederungskennzahl.string.replace_with(
                "999" + s_norm.gliederungskennzahl.string
            )
