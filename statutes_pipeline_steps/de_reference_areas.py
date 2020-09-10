import itertools
import os

import bs4
from regex import regex

from utils.common import (
    ensure_exists,
    list_dir,
    create_soup,
    stem_law_name,
    get_stemmed_law_names_for_filename,
)
from statics import (
    DE_REFERENCE_AREAS_PATH,
    DE_HELPERS_PATH,
    DE_REFERENCE_AREAS_LOG_PATH,
    DE_XML_PATH,
    DE_RVO_XML_PATH,
    DE_RVO_REFERENCE_AREAS_PATH,
    DE_RVO_HELPERS_PATH,
    DE_RVO_REFERENCE_AREAS_LOG_PATH,
)


def de_reference_areas_prepare(overwrite, regulations):
    src = DE_RVO_XML_PATH if regulations else DE_XML_PATH
    dest = DE_RVO_REFERENCE_AREAS_PATH if regulations else DE_REFERENCE_AREAS_PATH

    ensure_exists(dest)
    files = list_dir(src, ".xml")

    if not overwrite:
        existing_files = os.listdir(dest)
        files = list(filter(lambda f: f not in existing_files, files))
    return files


def de_reference_areas(filename, law_names, regulations):
    src = DE_RVO_XML_PATH if regulations else DE_XML_PATH
    dest = DE_RVO_REFERENCE_AREAS_PATH if regulations else DE_REFERENCE_AREAS_PATH

    laws_lookup = get_stemmed_law_names_for_filename(filename, law_names)
    logs = []
    laws_lookup_keys = sorted(laws_lookup.keys(), reverse=True)
    soup = create_soup(f"{src}/{filename}")
    para, art, misc = analyze_type_of_headings(soup)

    logs.extend(find_references_in_soup(soup, laws_lookup, laws_lookup_keys, para, art))

    # Find references without preceding article or § (currently not implemented)
    # long_law_regex_pattern = law_keys_to_regex(laws_lookup_keys, 5)
    # short_law_regex_pattern = law_keys_to_regex(laws_lookup_keys, 3, 4)
    # for section in soup.find_all("text"):
    #     find_law_references_in_section(
    #         section, soup, long_law_regex_pattern, stem_law_name
    #     )
    #     find_law_references_in_section(
    #         section, soup, short_law_regex_pattern, clean_name
    #     )

    save_soup_with_style(soup, f"{dest}/{filename}")

    return logs


def de_reference_areas_finish(logs_per_file, regulations):
    logs = list(itertools.chain.from_iterable(logs_per_file))
    ensure_exists(DE_RVO_HELPERS_PATH if regulations else DE_HELPERS_PATH)
    with open(
        DE_RVO_REFERENCE_AREAS_LOG_PATH if regulations else DE_REFERENCE_AREAS_LOG_PATH,
        mode="w",
    ) as f:
        f.write("\n".join(sorted(logs, key=lambda x: x.lower())))


#######
# Regex
#######

# fmt: off

reference_range_pattern_str = (
    r'(?(DEFINE)'
        r'(?<numb>'
            r'('
                r'\d+(?>\.\d+)*[a-z]?|'
                r'[ivx]+|'
                r'[a-z]\)?'
            r')'
            r'(\s?ff?\.|\s?ff\b|\b)'
        r')'
        r'(?<wordnumb>('
            r'erste|'
            r'zweite|'
            r'dritte|'
            r'letzte'
        r')r?s?)'
        r'(?<unit>'
            r'\bArt\b\.?|'
            r'Artikels?n?|'
            r'§{1,2}|'
            r'Nrn?\b\.?|'
            r'Nummer|'
            r'Abs\b\.?|'
            r'Absatz|'
            r'Absätze|'
            r'Unterabsatz|'
            r'Unterabs\b\.?|'
            r'S\b\.?|'
            r'Satz|'
            r'Sätze|'
            r'Ziffern?|'
            r'Ziffn?\b\.?|'
            r'Buchstaben?|' # Doppelbuchstabe and  DBuchst. missing (Problem in numb)
            r'Buchst\b\.?|'
            r'Halbsatz|'
            r'Teilsatz|'
            r'Abschnitte?|'
            r'Abschn\b\.?|'
            r'Alternativen?|'
            r'Alt\b\.?|'
            r'Anhang|'
            r'Anhänge'
        r')'
        r'(?<conn>,?'
            r'(?>'
                r',\s*|'
                r'\s+und\s+|'
                r'\s+sowie\s+|'
                r'\s+bis\s+|'
                r'\s+oder\s+|'
                r'(?>\s+jeweils)?(?>\s+auch)?\s+(?>in\s+Verbindung\s+mit|i\.?\s?V\.?\s?m\.?)\s+'
            r')'
            r'(?>nach\s+)?'
            r'(?>(?>der|des|den|die)\s+)?'
        r')'
    r')' # end DEFINE
    r'(?P<trigger>'
        r'('
            r'§{1,2}|'
            r'\bArt\b\.?|'
            r'Artikels?n?'
        r')\s*'
    r')'
    r'(?P<main>'
        r'(?&numb)'
        r'('
            r'\s*'
            r'('
                r'('
                    r'(?&conn)(?&unit)\s|'
                    r'(?&conn)|'
                    r'(?&unit)\s'
                r')'
                r'(?&numb)'
            r'|'
                r'(?&conn)?'
                r'(?&wordnumb)'
                r'\s+'
                r'(?&unit)'
            r')'
        r')*'
    r')?'
)
reference_range_pattern = regex.compile(
    reference_range_pattern_str,
    flags=regex.IGNORECASE
)


suffix_ignore_pattern_str = (
    r'^('
        r'(Gesetzes|Anordnung) vom \d+. \w+ \d+ \(BGBl\. I S\. \d+\)|'
        r'(G|AnO) v\. \d+\.\s?\d+\.\s?\d+ I+ \d+|'
        r'(saarländischen )?Gesetzes Nr\. \d+ ?[\w\s\-äöüÄÖÜ]{0,120} vom \d+. \w+ \d+ \(Amtsblatt des Saarlande?s S\. \d+\)|'
        r'(([\w\-äöüÄÖÜß]+|\d+\.) ){0,5}(Durchführungs)?verordnung zum [\w-äöüÄÖÜß]+gesetz(( in der Fassung der Bekanntmachung)? vom \d+. \w+ \d+ \(.{8,50}\))?|'
        r'([\w\-äöüÄÖÜß]{1,60}\s|\d+\.\s|Nr\.\s){0,8}?[\w\-äöüÄÖÜß]{3,60}(?<!\bver)(ordnung|gesetz|gesetze?s?buch|übereinkommen|statut|vertrag)(er|en|es|s)?(?=\b)(?! zum)( (von )?[\d/]+)?(( in der Fassung)?( der Bekanntmachung)? vom \d+. \w+ \d+ \(.{8,50}\))?|'
        r'[\w-äöüÄÖÜß]*tarifvertr(a|ä)ge?s?|'
        r'(abgelösten )?TV\s\w+|'
        r'Anlage\b|'
        r'([\wäöüÄÖÜß]+\s)?[\wäöüÄÖÜß]*(Gesetz|Übereinkommen|vereinbarung|verordnung|Abkommens|Vertrag|Konvention|Protokoll|Anordnung|Satzung|bestimmung|Verfassung)e?s?n?\s\s?(zur|über|vom|zum|zu dem|von|zwischen|des|der|betreffend)|'
        r'[\wäöüÄÖÜß]*-(vertrag|abkommen)e?s?|'
        r'(in\s(?!Artikels?n?)[\w\s\.]{2,100}?\s)?(vor)?(genannten|bezeichneten)\s\w*(Verordnung|Gesetz)e?s?n?|'
        r'in\s(?=(Art|§)[\w\s\.§]{2,100}?\s(vor)?(genannten|bezeichneten)\s\w*(Verordnung|Gesetz)e?s?n?)' # Similiar to pattern above, but stops before next reference trigger (Art...|§)
    r')'
)
suffix_ignore_pattern = regex.compile(
    suffix_ignore_pattern_str,
    flags=regex.IGNORECASE
)

# fmt: on


########################################
# Functions general and normal citations
########################################


def match_law_name(more_stemmed, laws, laws_keys_ordered):
    x = 1
    for law in laws_keys_ordered:
        if more_stemmed[: len(law)] == law:
            return law
    return None


def save_soup_with_style(soup, path):
    output_lines = str(soup).replace("\n\n", "\n").split("\n")
    output_lines.insert(1, '<?xml-stylesheet href="../../notebooks/xml-styles.css"?>')
    output = "\n".join(output_lines)

    with open(path, "w") as f:
        f.write(output)


def analyze_type_of_headings(soup):
    para = 0
    art = 0
    misc = 0
    for tag in soup.find_all("seqitem"):
        if "heading" not in tag.attrs:
            misc += 1
        elif tag.attrs["heading"].replace("\n", "").startswith("§"):
            para += 1
        elif tag.attrs["heading"].replace("\n", "").lower().startswith("art"):
            art += 1
        else:
            misc += 1
    return para, art, misc


def add_tag(string, pos, end, tag):
    tag.string = string[pos:end]
    return [
        bs4.element.NavigableString(string[:pos]),
        tag,
        bs4.element.NavigableString(string[end:]),
    ]


def split_reference(string, len_main, len_suffix, soup):
    main_str = string[:len_main]
    suffix_str = string[len_main : len_main + len_suffix]
    law_str = string[len_main + len_suffix :]

    result = [soup.new_tag("main"), soup.new_tag("suffix"), soup.new_tag("lawname")]
    result[0].append(main_str)
    result[1].append(suffix_str)
    result[2].append(law_str)

    return result


def get_dict_law_name_len(test_str, laws_lookup, laws_lookup_keys):
    test_str_stem = stem_law_name(test_str)
    match = match_law_name(test_str_stem, laws_lookup, laws_lookup_keys)
    if not match:
        return 0
    test_str_splitted = regex.findall(r"[\w']+|[\W']+", test_str)
    match_splitted = regex.findall(r"[\w']+|[\W']+", match)
    match_raw = "".join(test_str_splitted[: len(match_splitted)])
    assert len(test_str_splitted[0].strip()) > 0, (match, test_str, test_str_stem)

    # Filter if last matched word of law name does not continue after match with a string that would not be stemmed
    last_word_test_stemmed = stem_law_name(test_str_splitted[len(match_splitted) - 1])
    last_word_match = match_splitted[-1]
    if last_word_match != last_word_test_stemmed:
        # print("SKIPPING", match_raw, "-", match)
        return 0

    return len(match_raw)


def get_eu_law_name_len(test_str):
    match = regex.match(
        r"^("
        r"(Delegierten )?(Durchführungs)?(Verordnung|Richtlinie)\s?\((EU|EW?G|Euratom)\)\s+(Nr\.\s+)?\d+/\d+|"
        r"(Durchführungs)?(Richtlinie|Entscheidung)\s+\d+/\d+/(EW?G|EU)\b|"
        r"(Rahmen)?beschlusses\s\d+/\d+/\w\w\b"
        r")",
        test_str,
        flags=regex.IGNORECASE,
    )
    return len(match[0]) if match else 0


def get_ignore_law_name_len(test_str):
    match = suffix_ignore_pattern.match(test_str)
    #     if match: print(test_str[:200].replace("\n", "\\n"), file=debug_file)
    return len(match[0]) if match else 0


def get_no_suffix_ignore_law_name_len(test_str):
    match = regex.match(
        r"^("
        r"dieser Verordnung|"
        r"(G|AnO)\s?[i\d-\/]* v(om)?\.? \d+\.\s?\d+\.\s?\d+( I+)? [\d-]+"
        r")",
        test_str,
        flags=regex.IGNORECASE,
    )
    return len(match[0]) if match else 0


def get_sgb_law_name_len(test_str):
    # fmt: off
    match = regex.match(
        r"^("
            r"(erst|zweit|dritt|viert|fünft|sechst|siebt|acht|neunt|zehnt|elft|zwölft|\d{1,2}\.)"
            r"e(n|s)? buche?s?(( des)? sozialgesetzbuche?s?)?"
        r"|"
            r"SGB"
            r"(\s|\-)"
            r"("
                r"(I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII)\b"
            r"|"
                r"\d{1,2}"
            r")"
        r")",
        test_str,
        flags=regex.IGNORECASE,
    )
    # fmt: on
    #     if match: print(test_str[:200].replace("\n", "\\n"), file=debug_file)
    return len(match[0]) if match else 0


def get_suffix_and_law_name(string, laws_lookup, laws_lookup_keys):
    """
    Returns a tuple containing length of
    1. the article between numbers and law name (eg. " der ")
    2. length of name of law as in the given string
    If not found lengths are 0.
    """
    suffix_match = regex.match(r"^,?\s+?de[sr]\s+", string)

    if suffix_match:

        suffix_len = suffix_match.end()
        law_test = string[suffix_len : suffix_len + 1000]

        dict_suffix_len = get_dict_law_name_len(law_test, laws_lookup, laws_lookup_keys)
        if dict_suffix_len:
            return suffix_len, dict_suffix_len, "dict"

        sgb_suffix_len = get_sgb_law_name_len(law_test)
        if sgb_suffix_len:
            return suffix_len, sgb_suffix_len, "sgb"

        eu_suffix_len = get_eu_law_name_len(law_test)
        if eu_suffix_len:
            return suffix_len, eu_suffix_len, "eu"

        ignore_suffix_len = get_ignore_law_name_len(law_test)
        if ignore_suffix_len:
            return suffix_len, ignore_suffix_len, "ignore"

        return suffix_len, 0, "unknown"

    else:  # no der/des suffix
        suffix_match = regex.match(r"^[\s\n]+", string[:1000])
        if suffix_match:
            suffix_len = len(suffix_match[0])
            law_test = string[suffix_len:1000]

            dict_suffix_len = get_dict_law_name_len(
                law_test, laws_lookup, laws_lookup_keys
            )
            if dict_suffix_len:
                return suffix_len, dict_suffix_len, "dict"

            sgb_suffix_len = get_sgb_law_name_len(law_test)
            if sgb_suffix_len:
                return suffix_len, sgb_suffix_len, "sgb"

            ignore_no_suffix_len = get_no_suffix_ignore_law_name_len(law_test)
            if ignore_no_suffix_len:
                return suffix_len, ignore_no_suffix_len, "ignore"

        return 0, 0, "internal"


def handle_reference_match(
    match, section, soup, laws_lookup, laws_lookup_keys, para, art
):
    suffix_len, law_len, law_match_type = get_suffix_and_law_name(
        section.contents[-1][match.end() :], laws_lookup, laws_lookup_keys
    )

    # Set internal references to ignore if seqitem unit (Art|§) does not match between reference and target law
    if law_match_type == "internal":
        if (section.contents[-1][match.start() :].startswith("§") and para == 0) or (
            section.contents[-1][match.start() :].lower().startswith("art") and art == 0
        ):
            law_match_type = "ignore"

    ref_tag = soup.new_tag("reference", pattern="inline")
    section.contents[-1:] = add_tag(
        section.contents[-1], match.start(), match.end() + suffix_len + law_len, ref_tag
    )
    ref_tag.contents = split_reference(ref_tag.string, len(match[0]), suffix_len, soup)
    ref_tag.contents[-1]["type"] = law_match_type


def find_references_in_section(section, soup, laws_lookup, laws_lookup_keys, para, art):
    logs = []
    match = reference_range_pattern.search(section.contents[-1])  # Search first match
    while match:
        if match.groupdict()["main"]:
            handle_reference_match(
                match, section, soup, laws_lookup, laws_lookup_keys, para, art
            )
        match = reference_range_pattern.search(
            section.contents[-1], 0 if match.groupdict()["main"] else match.end()
        )
    return logs


def find_references_in_soup(
    soup, laws_lookup, laws_lookup_keys, para, art, text_tag_name="text"
):
    logs = []
    for text in soup.find_all(text_tag_name):
        if text.is_empty_element:
            continue
        assert text.string
        logs.extend(
            find_references_in_section(
                text, soup, laws_lookup, laws_lookup_keys, para, art
            )
        )
    return logs


#######################################################
# Functions: references without preceding 'article' or §
#######################################################


def pos_in_orig_string(i, stemmed, orig):
    prefix = stemmed[:i]
    stemmed_tokens = regex.findall(r"[\w']+|[\W']+", prefix)
    orig_tokens = regex.findall(r"[\w']+|[\W']+", orig)
    #     return len(''.join(orig_tokens[:len(stemmed_tokens)-1])) + len(stemmed_tokens[-1]) # Precise position
    return len("".join(orig_tokens[: len(stemmed_tokens)]))  # Round to next boundary


def law_keys_to_regex(keys, min_length, max_length=-1):
    pattern = ""
    for key in keys:
        if len(key) >= min_length and (len(key) <= max_length or max_length == -1):
            pattern += regex.escape(key) + r"|"
    pattern = pattern[:-1]
    full_pattern = r"\b(?>" + pattern + r")\b"
    return regex.compile(full_pattern, flags=regex.IGNORECASE)


def find_law_references_in_section(section, soup, law_regex_pattern, sanitizer):
    for item in list(section.contents):
        i_in_section = section.contents.index(item)
        if type(item) is not bs4.element.NavigableString:
            continue
        test_string = sanitizer(item.string)
        matches = law_regex_pattern.finditer(test_string)
        for match in reversed(list(matches)):
            orig_start = pos_in_orig_string(match.start(), test_string, item.string)
            orig_end = pos_in_orig_string(match.end(), test_string, item.string)

            ref_tag = soup.new_tag("reference", pattern="generic")

            section.contents[i_in_section : i_in_section + 1] = add_tag(
                section.contents[i_in_section], orig_start, orig_end, ref_tag
            )
