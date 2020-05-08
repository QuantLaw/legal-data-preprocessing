ALL_YEARS = list(range(1994, 2019))

US_DATA_PATH = "../legal-networks-data/us"
US_TEMP_DATA_PATH = "temp/us"

US_INPUT_PATH = f"{US_DATA_PATH}/1_input"
US_ORIGINAL_PATH = f"{US_TEMP_DATA_PATH}/11_htm"
US_XML_PATH = f"{US_TEMP_DATA_PATH}/12_xml"
US_REFERENCE_AREAS_PATH = f"{US_TEMP_DATA_PATH}/13_reference_areas"
US_REFERENCE_PARSED_PATH = f"{US_DATA_PATH}/2_xml"
US_HIERARCHY_GRAPH_PATH = f"{US_DATA_PATH}/3_hierarchy_graph"
US_CROSSREFERENCE_LOOKUP_PATH = f"{US_TEMP_DATA_PATH}/31_crossreference_lookup"
US_CROSSREFERENCE_EDGELIST_PATH = f"{US_TEMP_DATA_PATH}/32_crossreference_edgelist"
US_CROSSREFERENCE_GRAPH_PATH = f"{US_DATA_PATH}/4_crossreference_graph"

US_SNAPSHOT_MAPPING_EDGELIST_PATH = f"{US_DATA_PATH}/5_snapshot_mapping_edgelist"

US_HELPERS_PATH = f"{US_TEMP_DATA_PATH}/helpers"
US_REFERENCE_AREAS_LOG_PATH = f"{US_HELPERS_PATH}/us_extract_reference_areas.log"
US_REFERENCE_PARSED_LOG_PATH = f"{US_HELPERS_PATH}/us_extract_reference_parsed.log"


DE_DATA_PATH = "../legal-networks-data/de"
DE_TEMP_DATA_PATH = "temp/de"

JURIS_EXPORT_PATH = f"{DE_DATA_PATH}/1_juris_gii_xml"
JURIS_EXPORT_GESETZE_LIST_PATH = f"{DE_DATA_PATH}/1_juris_gii_xml_gesetze.txt"

DE_ORIGINAL_PATH = f"{DE_TEMP_DATA_PATH}/11_gii_xml"
DE_XML_PATH = f"{DE_TEMP_DATA_PATH}/12_xml"
DE_LAW_NAMES_PATH = f"{DE_TEMP_DATA_PATH}/12_xml_law_names.csv"
DE_LAW_NAMES_COMPILED_PATH = f"{DE_TEMP_DATA_PATH}/12_xml_law_names_compiled.pickle"
DE_REFERENCE_AREAS_PATH = f"{DE_TEMP_DATA_PATH}/13_reference_areas"
DE_REFERENCE_PARSED_PATH = f"{DE_DATA_PATH}/2_xml"
DE_HIERARCHY_GRAPH_PATH = f"{DE_DATA_PATH}/3_hierarchy_graph"
DE_CROSSREFERENCE_LOOKUP_PATH = f"{DE_TEMP_DATA_PATH}/31_crossreference_lookup"
DE_CROSSREFERENCE_EDGELIST_PATH = f"{DE_TEMP_DATA_PATH}/32_crossreference_edgelist"
DE_CROSSREFERENCE_GRAPH_PATH = f"{DE_DATA_PATH}/4_crossreference_graph"
DE_SNAPSHOT_MAPPING_EDGELIST_PATH = f"{DE_DATA_PATH}/5_snapshot_mapping_edgelist"

DE_HELPERS_PATH = f"{DE_TEMP_DATA_PATH}/helpers"
DE_REFERENCE_AREAS_LOG_PATH = f"{DE_HELPERS_PATH}/de_extract_reference_areas.log"
DE_REFERENCE_PARSED_LOG_PATH = f"{DE_HELPERS_PATH}/de_extract_reference_parsed.log"
