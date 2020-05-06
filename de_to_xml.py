import re
import traceback

from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString

from common import ensure_exists, list_dir, find_parent_with_name
from statics import DE_XML_PATH, DE_ORIGINAL_PATH


def de_to_xml_prepare(overwrite):
    ensure_exists(DE_XML_PATH)
    files = list_dir(DE_ORIGINAL_PATH, ".html")

    if not overwrite:
        existing_files = list_dir(DE_XML_PATH, ".xml")
        existing_files_sources = list(
            map(lambda x: x.replace(".xml", ".html"), existing_files)
        )

        files = list(filter(lambda f: f not in existing_files_sources, files))

    return sorted(files)


def de_to_xml(filename):
    try:
        target_filename = filename[: -len(".html")] + ".xml"
        with open(f"{DE_ORIGINAL_PATH}/{filename}") as f:
            body = BeautifulSoup(f.read(), "lxml").body
        convert_to_xml(body, f"{DE_XML_PATH}/{target_filename}")

    except (KeyError, AttributeError) as err:
        print(f"\n\nfile: {filename}")
        print(err)
        print(traceback.format_exc())


#######################################
# Functions
#######################################


def clean_whites(text):
    return re.sub(r"\n+\n", r"\n\n", text)


def clean_abs(text):
    return re.sub(r"<text>\n+</text>", r"", text)


def clean_toc(text):
    return re.sub("<heading>\nInhaltsübersicht.*?</text>", "", text, flags=re.DOTALL)


def clean_entry(text):
    return clean_whites(
        re.sub("<heading>\nEingangsformel.*?</text>", "", text, flags=re.DOTALL)
    )


def get_value_for_document_header(header_string):
    if not header_string:
        return None
    tr = find_parent_with_name(header_string, "tr")
    return tr.find("td", {"class": "TD70"}).getText().strip()


def get_abk(body):
    document_header = body.find("div", {"class": "documentHeader"})
    elem_jur_abk = document_header.find(text=re.compile("juris-Abk.rzung."))
    elem_amt_abk = document_header.find(text=re.compile("Amtliche Abk.rzung."))
    assert elem_amt_abk or elem_jur_abk
    elem_amt_abk = get_value_for_document_header(elem_amt_abk)
    elem_jur_abk = get_value_for_document_header(elem_jur_abk)

    assert not elem_amt_abk or '"' not in elem_amt_abk
    assert not elem_jur_abk or '"' not in elem_jur_abk
    return elem_amt_abk or elem_jur_abk or ""


def convert_to_xml(body, xml_filename):
    with open(xml_filename, "w", encoding="utf8") as f:
        print(f"<document>", file=f)
        for elem in body.findAll(["div", "h3"]):
            classes = elem.get("class") or []
            if "docLayoutTitel" in classes:
                print("<heading>", file=f)
                print(re.sub(r"[\n\s]+", " ", elem.getText(" ")).strip(), file=f)
                print("</heading>", file=f)
            elif "jurAbsatz" in classes:
                print("<text>", file=f)
                for el in elem.children:
                    if isinstance(el, Tag) and (
                        el.get("class", [""])[0] == "historie"
                        or el.get("class", [""])[0] == "anwendung"
                    ):
                        pass
                    elif isinstance(el, NavigableString):
                        print(el.replace("<", "&lt;").replace(">", "&gt;"), file=f)
                    else:
                        print(
                            " ".join(el.stripped_strings)
                            .replace("<", "&lt;")
                            .replace(">", "&gt;"),
                            file=f,
                        )
                print("</text>", file=f)
        print("</document>", file=f)
    with open(xml_filename, "r", encoding="utf8") as f:
        text = f.read()
    cleaned = clean_entry(clean_whites(clean_abs(clean_toc(text))))
    with open(xml_filename, "w", encoding="utf8") as f:
        f.write(cleaned)
