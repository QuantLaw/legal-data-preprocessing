[![codecov](https://codecov.io/gh/QuantLaw/legal-data-preprocessing/branch/master/graph/badge.svg?token=FABCUR680K)](https://codecov.io/gh/QuantLaw/legal-data-preprocessing)
[![Tests](https://github.com/QuantLaw/legal-data-preprocessing/workflows/Tests/badge.svg)](https://github.com/QuantLaw/legal-data-preprocessing/actions)
[![Maintainability](https://api.codeclimate.com/v1/badges/8cffa9a56ce357314456/maintainability)](https://codeclimate.com/repos/5f1bf2a3fccc45014c00c615/maintainability)
[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.4070772.svg)](https://doi.org/10.5281/zenodo.4070772)

# Legal Data Preprocessing

This repository contains code to preprocess legal text documents.
It is, inter alia, used to produce the results reported in the following publications:

- Daniel Martin Katz, Corinna Coupette, Janis Beckedorf, and Dirk Hartung, Complex Societies and the Growth of the Law, *Sci. Rep.* **10** (2020), [https://doi.org/10.1038/s41598-020-73623-x](https://doi.org/10.1038/s41598-020-73623-x)
- Corinna Coupette, Janis Beckedorf, Dirk Hartung, Michael Bommarito, and Daniel Martin Katz, Measuring Law Over Time, to appear (2021)

Related Repositories:
- [Complex Societies and the Growth of the Law](https://github.com/QuantLaw/Complex-Societies-and-Growth) ([Publication Release](https://doi.org/10.5281/zenodo.4070769))
- [Measuring Law Over Time](https://github.com/QuantLaw/Measuring-Law-Over-Time)
- [Legal Data Clustering](https://github.com/QuantLaw/legal-data-clustering) ([Latest Publication Release](https://doi.org/10.5281/zenodo.4070774))

Related Data: 
- [Preprocessed Input Data for *Sci. Rep.* **10** (2020)](https://doi.org/10.5281/zenodo.4070767)
- [Preprocessed Input Data for *Measuring Law Over Time*, to appear (2021)](https://doi.org/10.5281/zenodo.4660133)

## Setup

1. It is assumed that you have Python 3.7 installed. (Other versions are not tested.)
2. Set up a virtual environment and activate it. (This is not required but recommended.)
3. Install the required packages `pip install -r requirements.txt`.

## Getting Started

Make sure the following folders do not exist next to the root folder of this repository:
- `legal-networks-data`
- `gesetze-im-internet`

Download and prepare the data for the United States (US) and Germany. (See the respective "1. Data input"
sections below.) Afterwards, you can run the pipeline.

For the US statutory data:

1. Download the data: `python download_us_code_data.py`
2. Run all steps of the pipeline: `python . us all`

For the US statutory & regulatory data:

1. Download statutory data: `python download_us_code_data.py`
2. Download regulatory data: `python download_us_reg_data.py`
3. Run all steps of the pipeline: `python . us all` and `python . us all -r`


For the German statutory data, using a *juris* export:

1. Prepare the data (as shown in a separate repository)
2. Run all steps of the pipeline: `python . de all`

For the German statutory & regulatory data, using a *juris* export:

1. Prepare the data (as shown in a separate repository)
2. Run all steps of the pipeline: `python . de all -r`

For the German statutory data, using Gesetze im Internet (GII):

1. Prepare the data: `python download_de_gesetze_im_internet_data.py --dates 2019-06-10 2020-01-18`.
    You need to specify the dates you want to analyze.
2. Run all steps of the pipeline except for `prepare_input` for the specified dates:
    `python . de xml law_names reference_areas reference_parse hierarchy_graph crossreference_lookup crossreference_edgelist crossreference_graph snapshot_mapping_edgelist --snapshots 2019-06-10 2020-01-18`

If you need to reduce memory usage, you can deactivate multiprocessing with the argument `--single-process`.

To download and prepare German judicial decision data from https://www.rechtsprechung-im-internet.de,
run `python de_decisions_pipeline.py all`.


## Statutes

US and German federal statutes and regulations are converted from official sources (or *juris*)
to multiple clean formats focussing on the structure of the law.

Output formats are:

- XML files containing the text, the hierarchical structure of the law, and cross-references.
- Gpickle files for each Title/Gesetz/Rechtsverodnung and version containing the hierarchical structure of the statutes.
- Gpickle files for each snapshot (year in the US or date in Germany) containing the hierarchical structure of the statutes
    and the cross-references between different elements of the statutes with reduced granularity and corresponding nodelists 
    and edgelists.
- Snapshot mapping edgelists: These lists map elements of a network at one snapshot
    to a snapshot at another time. They encode, e.g., where a clause of the US Code in 2010 is
    located in the US Code of 2011. This mapping is derived from the text and the structure
    of the statutes.

The steps of the pipeline are:

- `prepare_input`
- `xml`
- `law_names` (only for German pipeline)
- `reference_areas`
- `reference_parse`
- `hierarchy_graph`
- `crossreference_lookup`
- `crossreference_edgelist`
- `crossreference_graph`
- `snapshot_mapping_index`
- `snapshot_mapping_edgelist`


### US

The processing for the US Code is executed in multiple steps:


#### 1. Data Input

Inputs are ZIP files downloaded from the US House of Representatives Office of the Law
Revision Counsel and U.S. Government Publishing Office. We use annual versions in XHTML format that are available on
https://uscode.house.gov/download/annualhistoricalarchives/downloadxhtml.shtml and
https://www.govinfo.gov/bulkdata/CFR.
Files should be located at regarding the statutes `../legal-networks-data/us/1_input` and regarding the regulations `../legal-networks-data/us_reg/1_input`.
This folder should contain unzipped yearly folders.

You can automatically obtain the required data by running `download_us_code_data.py` and `download_us_reg_data.py`.


#### 2. XML Files

- Files containing titles of the US Code are copied to `temp/us/11_htm`.
    Appendices and Stylesheets are filtered. (Result of step: `prepare_input`)
- Simple XML files focusing on the structure are generated from the XHTML files.
    Results can be found in `temp/us/12_xml`. (Result of step: `xml`)
- Text segments containing a cross-reference are annotated in the XML files. Results are saved to
    `temp/us/13_reference_areas`. (Result of step: `reference_areas`)
- The contents of the annotated cross-references are extracted and added to the XML.

The results of the XML generation are saved to `../legal-networks-data/us/2_xml`. (Result of step: `reference_parse`)

CFR data is located at `us_reg` folders next to the `us` folder.


#### 3. Hierarchy Graphs

Graphs containing the hierarchical structure of the statutes are saved to `../legal-networks-data/us/3_hierarchy_graph`
in separate files for each Title and annual version. (Result of step: `hierarchy_graph`)

CFR data is located at `us_reg` folders next to the `us` folder.


#### 4. Crossreference Graphs

- A list of all sections in the US Code at a specific point in time is generated to obtain a list of possible
    destinations of cross-references. This is a preparation step for drawing edges from the cross-reference source to the cross-reference destination. The lists are stored at `temp/us/31_crossreference_lookup`.
    (Result of step: `crossreference_lookup`)
- Lists of all cross-references are generated. They contain the ID of the referencing and the referenced element.
    The lists are located at `temp/us/32_crossreference_edgelist`.
    (Result of step: `crossreference_edgelist`)
- Hierarchy graphs of the individual Titles are combined and edges for cross-references are added within and between
    Titles.

Each annual version of the US Code is stored at `../legal-networks-data/us/4_crossreference_graph` in three files as a nodeslist, an edgelist and networkx graph stored as gpickle.gz-file in the subfolder `seqitems`. The node-list contains all nodes, whereas subseqitems are excluded in the networkx file.
(Result of step: `crossreference_graph`)

The combined data regarding the US Code and the CFR is located at `us_reg` folders next to the `us` folder.


#### 5. Snapshot Mapping Edgelists

Snapshot mapping edgelists are stored at `../legal-networks-data/us/5_snapshot_mapping_edgelist` and ``../legal-networks-data/us_reg/5_snapshot_mapping_edgelist``.


#### Germany

#### 1. Data Input

Inputs are XML files in a format simplified from that of documents available from GII.
These files can be generated from two sources:

1. XML files provided by GII. To obtain older versions of this website
    use our public archive at https://github.com/legal-networks/gesetze-im-internet.
    Downloaded files must be simplified before they are suitable input.
    Use `download_de_gesetze_im_internet_data.py` to download, simplify and rename the source files.
    This replaces step `prepare_input` in the pipeline.
    (Make sure that you do not run this step. It is not possible to run `all` steps.)
2. An export from the *juris* database can be used to obtain the data.
    Whereas this datasource covers a longer time period, we cannot make it publicly available due to licensing restrictions.

#### 2. XML Files

- Files in the simplified format of Gesetze im Internet are generated and saved to `temp/de/11_gii_xml`
    (Result of step: `prepare_input` or  `download_de_gesetze_im_internet_data.py`)
- Simple XML files focusing on the structure are generated from the original XML files.
    Results can be found in `temp/de/12_xml`. (Result of step: `xml`)
- A list of the names of all statutes (Gesetze) is saved to
        `temp/de/12_xml_law_names.csv` with a mapping to the corresponding files.
        This is used to extract cross-references, as statutes are typically referenced by their name.
        Names are saved in a stemmed version. (Result of step: `law_names`)

    Furthermore, `temp/de/12_xml_law_names_compiled.pickle` is generated.
        It contains the same information as `12_xml_law_names.csv`,
        but is optimized to obtain the stemmed names of all valid laws at specific dates. (Result of step: `law_names`)
- Text segments containing a cross-reference are annotated in the XML files. Results are saved to
    `temp/de/13_reference_areas`. (Result of step: `reference_areas`)
- The contents of the annotated cross-references are extracted and added to the XML.

The results of the XML generation are saved to `../legal-networks-data/de/2_xml`. (Result of step: `reference_parse`)

The combined data of statutes and regulations is located at `de_reg` folders next to the `de` folder.

#### 3. Hierarchy Graphs

Hierarchy Graphs are saved to `../legal-networks-data/de/3_hierarchy_graph`.
See the documentation regarding the US hierarchy graphs for further information.

The combined data of statutes and regulations is located at `de_reg` folders next to the `de` folder.

#### 4. Cross-Reference Graphs

In general cross-reference graphs are generated in the same manner as for the US dataset
(see above for further information).
(Interim) results are saved to
`temp/us/31_crossreference_lookup`,
`temp/us/32_crossreference_edgelist`, and
`../legal-networks-data/us/4_crossreference_graph`, respectively.

A major difference are the possible dates for which to create cross-reference graphs.
For the US, only annual version are available.
The *juris* export allows one to select any day to create a snapshot.
If you rely on https://github.com/legal-networks/gesetze-im-internet as a data source, you can only select days
for which a snapshot was created.

The combined data of statutes and regulations is located at `de_reg` folders next to the `de` folder.

#### 5. Snapshot Mapping Edgelists

Snapshot mapping edgelists are stored at `../legal-networks-data/de/5_snapshot_mapping_edgelist`.

The combined data of statutes and regulations is located at `de_reg` folders next to the `de` folder.
