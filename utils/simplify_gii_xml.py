import re

from bs4 import BeautifulSoup, NavigableString


def simplify_gii_xml(source, destination):
    soup = load_soup(source)
    simplify(soup)
    save_soup(soup, destination)


def load_soup(source):
    with open(source) as f:
        return BeautifulSoup(f.read(), "lxml-xml")


def save_soup(soup, destination):
    with open(destination, "w") as f:
        f.write(str(soup))
        # f.write(soup.prettify())


def remove_new_lines(tag, soup):
    for descendant in list(tag.descendants):
        if type(descendant) is NavigableString:
            text = str(descendant)
            text = re.sub(r"\s+", " ", text).strip()
            if str(descendant) != text:
                descendant.replaceWith(soup.new_string(text))


def simplify(soup: BeautifulSoup):
    # General
    for t in soup.find_all(attrs={"builddate": True}):
        del t.attrs["builddate"]

    for t in soup.find_all("FnR"):
        t.extract()

    for metadaten in soup.find_all("metadaten"):
        for t in metadaten.find_all("titel", recursive=False):
            del t.attrs["format"]
        for t in metadaten.find_all("enbez", recursive=False):
            if t.string == "(XXXX)":
                t.string = "XXXX"

    for t in soup.find_all("BR"):
        text = " "
        if type(t.previous_sibling) is NavigableString:
            text = t.previous_sibling.string + " "
            t.previous_sibling.extract()
        if type(t.next_sibling) is NavigableString:
            text += t.next_sibling.string
            t.next_sibling.extract()

        t.replaceWith(soup.new_string(text))

    # Metadaten
    for tag_name in ["ausfertigung-datum", "fundstelle", "standangabe"]:
        for t in soup.metadaten.find_all(tag_name):
            t.extract()

    # Text
    for t in soup.find_all("SUP", attrs={"class": "Rec"}):
        t.replaceWith(soup.new_string(" "))

    for t in soup.find_all(["DT", "DD", "entry", "LA"]):
        t.insert(0, soup.new_string(" "))
        t.append(soup.new_string(" "))

    for t in soup.find_all("P"):
        new_t = soup.new_tag("P")
        text = t.get_text()
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            new_t.string = text
        t.replaceWith(new_t)

    for toc in soup.find_all("TOC"):
        text = toc.get_text(" ")
        text = re.sub(r"\s+", " ", text).strip()
        new_toc = soup.new_tag("TOC")
        new_toc.string = text
        toc.replaceWith(new_toc)

    for textdaten in soup.find_all("textdaten"):
        if textdaten.Footnotes:
            textdaten.Footnotes.extract()

        t = textdaten.find("text", recursive=False)
        if t and not t.get_text().strip():
            t.extract()

    for t in soup.find_all("Content"):
        if (
            type(t.next_sibling) is NavigableString
            and not t.next_sibling.string.strip()
        ):
            t.next_sibling.extract()

    for t in soup.find_all(["gliederungstitel", "titel", "langue", "kurzue"]):
        remove_new_lines(t, soup)
        for descendant in list(t.descendants):
            if type(descendant) is NavigableString:
                text = str(descendant)
                text = re.sub(r"\s*\*\)\s*$", "", text).strip()
                if str(descendant) != text:
                    descendant.replaceWith(soup.new_string(text))

    for t in soup.find_all("fussnoten"):
        t.extract()
