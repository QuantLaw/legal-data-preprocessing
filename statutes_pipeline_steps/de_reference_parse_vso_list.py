import json

import regex

# fmt: off
from quantlaw.de_extract.statutes_parse import StatutesParser
from quantlaw.de_extract.stemming import stem_law_name

reference_trigger_pattern = regex.compile(
    r'('
        r'§{1,2}|'
        r'\bArt\b\.?|'
        r'Artikels?n?'
    r')\s*'
)
# fmt: on


def identify_reference_in_juris_vso_list(soup, parser: StatutesParser):

    vso_tags = soup.find_all(["document", "seqitem"], attrs={"verweise": True})
    for vso_tag in vso_tags:
        parsed_vso_refs = []
        parsed_vso_refs_simple = []
        verweise = (
            []
            if vso_tag.attrs["verweise"] == "[]"
            else json.loads(vso_tag.attrs["verweise"])
        )
        for verweis in verweise:
            if not verweis["typ"] in [
                "Ermächtigung",
                "Rechtsgrundlage",
                "Durchführungsvorschrift",
            ]:
                # 'Vertragsgesetz', 'Sonderregelung', 'GLIEDERUNG', 'SAMMELVERWEISUNG',
                # 'Einführungsvorschrift', 'InnerstaatlDurchfVorschr' will be ignored
                continue
            if not verweis["normabk"]:
                continue
            lawname_stem = stem_law_name(verweis["normabk"])
            match = parser.match_law_name(lawname_stem)
            print(match)
            # if match:
            #     lawid = parser.laws_lookup[match]
            #     parsed_vso_ref = [[["Gesetz", lawid]]]
            #     parsed_vso_ref_simple = [[lawid]]
            #
            #     # Append ref. details if present in raw data
            #     enbez = verweis["enbez"]
            #     if enbez and reference_trigger_pattern.match(enbez):
            #
            #         try:
            #             (
            #                 reference_paths,
            #                 reference_paths_simple,
            #             ) = parse_reference_string(enbez, debug_context=None)
            #
            #             parsed_vso_ref = [
            #                 parsed_vso_ref[0] + r for r in reference_paths
            #             ]
            #             parsed_vso_ref_simple = [
            #                 parsed_vso_ref_simple[0] + r
            #                 for r in reference_paths_simple
            #             ]
            #
            #         except StringCaseException as error:
            #             print(error, "context", enbez)
            #
            #     parsed_vso_refs.extend(parsed_vso_ref)
            #     parsed_vso_refs_simple.extend(parsed_vso_ref_simple)

        # Remove duplicates
        parsed_vso_refs = remove_duplicate_references(parsed_vso_refs)
        parsed_vso_refs_simple = remove_duplicate_references(parsed_vso_refs_simple)

        vso_tag.attrs["parsed_verbose"] = json.dumps(
            parsed_vso_refs, ensure_ascii=False
        )
        vso_tag.attrs["parsed"] = json.dumps(parsed_vso_refs_simple, ensure_ascii=False)


def remove_duplicate_references(references):
    res = []
    res_str = []
    for elem in references:
        elem_str = str(elem)
        if elem_str not in res_str:
            res.append(elem)
            res_str.append(elem_str)

    return res
