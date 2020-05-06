ALL_YEARS = list(range(1994, 2019))

US_DATA_PATH = "../legal-networks-data/us"
US_TEMP_DATA_PATH = "temp/us"

US_INPUT_PATH = f"{US_DATA_PATH}/1_downloads"
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

DE_XML_PATH = f"{DE_TEMP_DATA_PATH}/11_xml"
DE_XML_NESTED_PATH = f"{DE_TEMP_DATA_PATH}/12_xml"
DE_LAW_NAMES_PATH = f"{DE_TEMP_DATA_PATH}/13_law_names.csv"
DE_REFERENCE_AREAS_PATH = f"{DE_TEMP_DATA_PATH}/14_reference_areas"
DE_REFERENCE_PARSED_PATH = f"{DE_DATA_PATH}/2_xml"
DE_HIERARCHY_GRAPH_PATH = f"{DE_DATA_PATH}/3_hierarchy_graph"
DE_LAW_VALIDITIES_PATH = (
    f"{DE_TEMP_DATA_PATH}/8_validities.csv"
)  # TODO later renumber to place after law_names
DE_CROSSREFERENCE_LOOKUP_PATH = f"{DE_TEMP_DATA_PATH}/31_crossreference_lookup"
DE_CROSSREFERENCE_EDGELIST_PATH = f"{DE_TEMP_DATA_PATH}/32_crossreference_edgelist"
DE_CROSSREFERENCE_GRAPH_PATH = f"{DE_DATA_PATH}/4_crossreference_graph"
DE_SNAPSHOT_MAPPING_EDGELIST_PATH = f"{DE_DATA_PATH}/5_snapshot_mapping_edgelist"

DE_HELPERS_PATH = f"{DE_TEMP_DATA_PATH}/helpers"
DE_REFERENCE_AREAS_LOG_PATH = f"{DE_HELPERS_PATH}/de_extract_reference_areas.log"
DE_REFERENCE_PARSED_LOG_PATH = f"{DE_HELPERS_PATH}/de_extract_reference_parsed.log"

@property
def DE_ORIGINAL_VERSION_INDICES_PATH():
    raise Exception('Outdated TODO')

@property
def DE_INPUT_PATH():
    raise Exception('Outdated TODO')

@property
def DE_INPUT_LIST_PATH():
    raise Exception('Outdated TODO')

@property
def DE_XML_HEADING_ORDERS_PATH():
    raise Exception('Outdated TODO')
