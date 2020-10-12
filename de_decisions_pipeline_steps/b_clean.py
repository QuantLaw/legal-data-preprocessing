import multiprocessing
import os

from bs4 import BeautifulSoup, Tag

from de_decisions_pipeline_steps.common import get_docparts_with_p
from statics import DE_DECISIONS_DOWNLOAD_XML, DE_DECISIONS_XML
from utils.common import ensure_exists, list_dir, save_soup


def clean_abs(section_tag):
    contents = []

    for dl in section_tag.findAll("dl"):
        number = dl.dt.get_text(" ").strip()
        number = number if len(number) else None
        text = dl.dd.get_text(" ").strip()
        indented = bool(dl.dd.p and "margin-left" in dl.dd.p.attrs.get("style", ""))
        if len(text):
            contents.append(
                dict(
                    number=number,
                    content=text,
                    indented=indented,
                )
            )

    return contents


def replace_tag_with_content(tag, contents, soup):
    for children in tag.contents:
        if type(children) is Tag:
            children.decompose()
    tag.contents = []
    for content_dict in contents:
        p_tag = soup.new_tag("p")
        if content_dict["indented"]:
            p_tag["indented"] = str(True)
        if content_dict["number"]:
            p_tag["numbers"] = content_dict["number"]
        p_tag.append(soup.new_string(content_dict["content"]))
        tag.append(p_tag)


def fix_data(decision, text):
    if "JURE149015016" in decision:
        text = text.replace("Art.l ", "Art. I ")
    return text


def clean_decision(decision):
    if not os.path.exists(f"{DE_DECISIONS_XML}/{decision}"):
        with open(f"{DE_DECISIONS_DOWNLOAD_XML}/{decision}", encoding="utf8") as f:
            content = f.read()
            content = content.replace("\xa0", " ")
            soup = BeautifulSoup(content, "lxml-xml")
        for doc_parts in get_docparts_with_p(soup):
            contents = clean_abs(doc_parts)
            replace_tag_with_content(doc_parts, contents, soup)

        soup_str = fix_data(decision, str(soup))
        save_soup(soup_str, f"{DE_DECISIONS_XML}/{decision}")


def clean():
    ensure_exists(DE_DECISIONS_XML)
    decisions = list_dir(DE_DECISIONS_DOWNLOAD_XML, ".xml")
    with multiprocessing.Pool() as p:
        p.map(clean_decision, decisions)
