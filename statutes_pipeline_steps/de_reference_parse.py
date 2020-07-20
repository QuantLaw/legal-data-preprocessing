import itertools
import json
import os
from collections import Counter

from regex import regex

from utils.common import (
    ensure_exists,
    list_dir,
    create_soup,
    save_soup,
    stem_law_name,
    get_stemmed_law_names_for_filename,
)
from statics import (
    DE_REFERENCE_AREAS_PATH,
    DE_HELPERS_PATH,
    DE_REFERENCE_PARSED_LOG_PATH,
    DE_REFERENCE_PARSED_PATH,
)


def de_reference_parse_prepare(overwrite):
    ensure_exists(DE_REFERENCE_PARSED_PATH)
    files = list_dir(DE_REFERENCE_AREAS_PATH, ".xml")

    if not overwrite:
        existing_files = os.listdir(DE_REFERENCE_PARSED_PATH)
        files = list(filter(lambda f: f not in existing_files, files))

    return files


def de_reference_parse(filename, law_names):
    laws_lookup = get_stemmed_law_names_for_filename(filename, law_names)
    laws_lookup_keys = sorted(laws_lookup.keys(), reverse=True)

    logs = list()

    # for debug
    logs.append(f"Start file - {filename}")

    soup = create_soup(f"{DE_REFERENCE_AREAS_PATH}/{filename}")
    parse_reference_content_in_soup(soup, debug_context=filename)
    current_lawid = soup.document.attrs["key"].split("_")[1]
    identify_reference_law_name_in_soup(
        soup, laws_lookup, laws_lookup_keys, current_lawid
    )
    identify_lawreference_law_name_in_soup(soup, laws_lookup)

    save_soup(soup, f"{DE_REFERENCE_PARSED_PATH}/{filename}")
    return logs


def de_reference_parse_finish(logs_per_file):
    logs = list(itertools.chain.from_iterable(logs_per_file))
    ensure_exists(DE_HELPERS_PATH)
    with open(DE_REFERENCE_PARSED_LOG_PATH, mode="w") as f:
        f.write("\n".join(sorted(logs, key=lambda x: x.lower())))


#######
# Regex
#######

unit_patterns = {
    r"§{1,2}": "§",
    r"Art\b\.?|[Aa]rtikels?n?": "Art",
    r"Nr\b\.?|Nummer|Nrn?\b\.?": "Nr",
    r"[Aa][Bb]s\b\.?|Absatz|Absätze": "Abs",
    r"Unter[Aa]bsatz|Unter[Aa]bs\b\.?": "Uabs",
    r"S\b\.?|Satz|Sätze": "Satz",
    r"Ziffern?|Ziffn?\b\.?": "Ziffer",
    r"Buchstaben?|Buchst\b\.?": "Buchstabe",
    r"Halbsatz": "Halbsatz",
    r"Teilsatz": "Teilsatz",
    r"Abschnitte?|Abschn\b\.?": "Abschnitt",
    r"Alternativen?|Alt\b\.?": "Alternative",
    r"Anhang|Anhänge": "Anhang",
}


###########
# Functions
###########


def stem_unit(unit):
    for unit_pattern in unit_patterns:
        if regex.fullmatch(unit_pattern, unit):
            return unit_patterns[unit_pattern]
    raise Exception(unit)


def is_unit(token):
    return regex.fullmatch("|".join(unit_patterns.keys()), token)


def is_pre_numb(token):
    # fmt: off
    return regex.fullmatch(
        r"("
            r"erste|"
            r"zweite|"
            r"dritte|"
            r"letzte"
        r")r?s?",
        token,
        flags=regex.IGNORECASE,
    )
    # fmt: on


def is_numb(token):
    # fmt: off
    return regex.fullmatch(
        r"(" 
            r"\d+(?>\.\d+)*[a-z]?|"
            r"[ivx]+|"
            r"[a-z]\)?"
        r")"
        r"("
            r"ff?\.|"
            r"ff\b|"
            r"(?<=[a-z])\)|"
            r"\b"
        r")",
        token,
        flags=regex.IGNORECASE,
    )
    # fmt: on


def fix_errors_in_citation(citation):
    result = regex.sub(r"\s+", " ", citation)
    result = regex.sub(
        r"§(?=\d)", "§ ", result
    )  # TODO this fix is probably not necessary anymore
    result = regex.sub(
        r",\sbis\s", " bis ", result
    )  # TODO improve handling as iit might result in false parsing e.g. § 8 Abs. 2 Nr. 1, bis 3
    return result


def split_citation_into_enum_parts(citation):
    """
    Citation is into enumerative parts. The enumerative part consists of a list.
    In most cases the list contains only one string.
    If the list contains two strings, the part refers to a range.
    """
    # fmt: off
    enum_parts = regex.split(
        r"(?>\s*,?(?>" r",\s*|" r"\s+und\s+|" r"\s+sowie\s+|"
        #             r'\s+bis\s+|'
        r"\s+oder\s+|"
        r"(?>\s+jeweils)?(?>\s+auch)?\s+(?>in\s+Verbindung\s+mit|i\.?\s?V\.?\s?m\.?)\s+"
        r"))"
        r"(?>nach\s+)?"
        r"(?>(?>der|des|den|die)\s+)?",
        citation,
        flags=regex.IGNORECASE,
    )
    # fmt: on

    # Split range
    enum_parts = [regex.split(r"\s*,?\s+bis\s+", part) for part in enum_parts]
    return enum_parts


class StringCaseException(Exception):
    pass


def split_citation_part(string):
    # fmt: off
    string = regex.sub(
        r"(" 
            r"\d+(?>\.\d+)?[a-z]?|"
            r"\b[ivx]+|" 
            r"\b[a-z]\)?"
        r")"
        r"(\sff?\.|\sff\b)",
        r"\1ff.",
        string,
        flags=regex.IGNORECASE,
    )
    # fmt: on
    tokens = regex.split(
        r"\s|(?<=Art\.|Art\b|Artikeln|Artikel)(?=\d)|(?<=§)(?=[A-Z0-9])",
        string,
        flags=regex.IGNORECASE,
    )
    while len(tokens) > 0:
        token = tokens.pop(0)
        if is_unit(token):
            if len(tokens) > 0:
                unit = stem_unit(token)
                token = tokens.pop(0)
                numb = token
                assert is_numb(numb), numb
            else:  # when citation ends with unit
                print(f"Citation {string} ends with unit {token}. Ignoring last unit.")
                break

        elif is_pre_numb(token):
            numb = token
            token = tokens.pop(0)
            assert is_unit(token)
            unit = stem_unit(token)
        elif is_numb(token):
            #             assert len(reference_paths) > 0
            unit = None
            numb = token
        else:
            raise StringCaseException(token, "in", string)
        numb = regex.sub(r"(ff?\.|ff|\))$", "", numb)
        yield [unit, numb]


def split_parts_accidently_joined(reference_paths):
    new_reference_paths = []
    main_unit = (
        "Art"
        if Counter([part[0] for part in itertools.chain(*reference_paths)]).get("Art")
        else "§"
    )
    for reference_path in reference_paths:
        temp_path = []
        for part in reference_path:
            if part[0] == main_unit:
                if len(temp_path):
                    new_reference_paths.append(temp_path)
                temp_path = []
            temp_path.append(part)
        new_reference_paths.append(temp_path)
    return new_reference_paths


def infer_units(reference_path, prev_reference_path):
    prev_path_units = [o[0] for o in prev_reference_path]
    if reference_path[0][0]:
        pass
    elif len(reference_path) > 1:
        try:
            prev_unit_index = prev_path_units.index(reference_path[1][0])
            # if not prev_unit_index > 0:
            #     print(f'Infer unit error: {citation}')
            reference_path[0][0] = prev_path_units[prev_unit_index - 1]
        except ValueError:
            reference_path[0][0] = prev_path_units[-1]
    else:
        reference_path[0][0] = prev_path_units[-1]

    try:
        prev_unit_index = prev_path_units.index(reference_path[0][0])
        reference_path[0:0] = prev_reference_path[:prev_unit_index]
    except:
        reference_path[0:0] = prev_reference_path


#         print(f'Insert all error: {citation}')


def parse_reference_content(reference):
    citation = reference.main.get_text().strip()
    citation = fix_errors_in_citation(citation)

    # For debug
    # print(citation + ' ' + reference.lawname.get_text(), file=debug_file)

    enum_parts = split_citation_into_enum_parts(citation)

    reference_paths = []
    for enum_part in enum_parts:
        for string in enum_part:
            splitted_citation_part_list = list(split_citation_part(string))
            if len(splitted_citation_part_list):
                reference_paths.append(splitted_citation_part_list)
            else:
                print(f"Empty citation part in {citation} in part {string}")

    reference_paths = split_parts_accidently_joined(reference_paths)

    for reference_path in reference_paths[1:]:
        prev_reference_path = reference_paths[reference_paths.index(reference_path) - 1]
        infer_units(reference_path, prev_reference_path)

    reference["parsed_verbose"] = json.dumps(reference_paths, ensure_ascii=False)
    reference_paths_simple = [
        [component[1] for component in path] for path in reference_paths
    ]
    reference["parsed"] = json.dumps(reference_paths_simple, ensure_ascii=False)


def parse_reference_content_in_soup(soup, debug_context=None):
    for reference in soup.find_all("reference", {"pattern": "inline"}):
        if reference.main:
            try:
                parse_reference_content(reference)
            except StringCaseException as error:
                print(error, "context", debug_context)


def generate_sgb_dict():
    sgb_dict_word = [
        "erst",
        "zweit",
        "dritt",
        "viert",
        "fuenft",
        "sechst",
        "siebt",
        "acht",
        "neunt",
        "zehnt",
        "elft",
        "zwoelft",
    ]

    sgb_dict_roman = [
        "i",
        "ii",
        "iii",
        "iv",
        "v",
        "vi",
        "vii",
        "viii",
        "ix",
        "x",
        "xi",
        "xii",
    ]

    sgb_dict = {}

    for idx in range(12):
        nr = idx + 1
        word = sgb_dict_word[idx]
        roman = sgb_dict_roman[idx]
        if nr in {9, 10}:
            value = (f"SGB-{roman.upper()}", f"SGB-{nr}")
        else:
            value = f"SGB-{nr}"
        sgb_dict[f"{word} buch"] = value
        sgb_dict[f"{word} buch sozialgesetzbuch"] = value
        sgb_dict[f"{word} buch d sozialgesetzbuch"] = value
        sgb_dict[f"sgb {roman}"] = value
        sgb_dict[f"sgb {nr}"] = value
        sgb_dict[f"{nr}. buch sozialgesetzbuch"] = value
        sgb_dict[f"sgb-{roman}"] = value
        sgb_dict[f"sgb-{nr}"] = value

    return sgb_dict


sgb_dict = generate_sgb_dict()


def match_law_name(more_stemmed, laws, laws_keys_ordered):
    for law in laws_keys_ordered:
        if more_stemmed[: len(law)] == law:
            return law
    return None


def identify_reference_law_name_in_soup(
    soup, laws_lookup, laws_lookup_keys, current_lawid
):
    for reference in soup.find_all("reference", {"pattern": "inline"}):

        if reference.lawname["type"] == "dict":
            lawname_stem = stem_law_name(reference.lawname.string)
            match = match_law_name(lawname_stem, laws_lookup, laws_lookup_keys)
            lawid = laws_lookup[match]

        elif reference.lawname["type"] == "sgb":
            lawid = sgb_dict[stem_law_name(reference.lawname.string)]
            if type(lawid) is tuple:
                assert len(lawid) == 2
                if lawid[0] in laws_lookup_keys:
                    lawid = lawid[0]
                elif lawid[1] in laws_lookup_keys:
                    lawid = lawid[1]
                else:
                    lawid = lawid[1]  # TODO remove for final
                    # raise Exception(lawid) # TODO activate

        elif reference.lawname["type"] == "internal":
            if current_lawid is None:
                raise Exception(
                    f"Current law id must be set for internal reference: {str(reference)}"
                )
            lawid = current_lawid
        else:
            continue  # ignore or unknown

        ref_parts = json.loads(reference["parsed_verbose"])
        for ref_part in ref_parts:
            ref_part.insert(0, ["Gesetz", lawid])
        reference["parsed_verbose"] = json.dumps(ref_parts, ensure_ascii=False)

        ref_parts = json.loads(reference["parsed"])
        for ref_part in ref_parts:
            ref_part.insert(0, lawid)
        reference["parsed"] = json.dumps(ref_parts, ensure_ascii=False)


def identify_lawreference_law_name_in_soup(soup, laws_lookup):
    for reference in soup.find_all("reference", {"pattern": "generic"}):
        reference["parsed"] = [[laws_lookup[stem_law_name(reference.string)]]]
