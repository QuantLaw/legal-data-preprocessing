# legal-data-preprocessing

## Installation

You need Python 3.7. Other python versions might work as well, but are not tested.
Install the dependencies with `pip install -r requirements.txt`.

## Getting started

Make the following folders do not exist next the root folder of this repository: 
- `legal-networks-data`
- `gesetze-im-internet`

Download and prepare the date for US and/or Germany. (See the respecive "1. Data input"
sections below.) Afterwards you can run the pipeline.

For the US data:

1. Download the data: `python download_us_code_data.py`
2. Run the all steps of the pipeline: `python . us all`

For the German data using the juris export:

1. Prepare the data (as shown in a separate repository)
2. Run the all steps of the pipeline: `python . de all`

For the German data using Gesetze im Internet:

1. Prepare the data. `python download_de_gesetze_im_internet_data.py --dates 2019-06-10 2020-01-18`.
    You need to specify the dates you want to analyse. 
2. Run the all steps but `prepare_input` of the pipeline for the specified dates: 
    `python . de xml law_names reference_areas reference_parse hierarchy_graph crossreference_lookup crossreference_edgelist crossreference_graph snapshot_mapping_edgelist --snapshots 2019-06-10 2020-01-18`
    
If you need to reduce memory usage, your can deactivate multiprocessing with the argument `--single-process`.


## Statutes

US and German federal statutes are converted from official sources (or juris) 
to multiple clean formats focussing on the structure of the law.

Output formats are:

- XML files containing the text, the hierarchical structure of the law and cross-references.
- GraphML files for each Title/Gesetz and version containing the hierarchical structure of the statutes
- GraphML files for each snapshot containing the hierarchical structure of the statutes 
    and the cross-references between different elements of the statutes.
- Snapshot mapping edgelists: These lists map elements of a network at one snapshot 
    to a snapshot at another time. It encodes e.g. where a clause of the US Code in 2010 is 
    located in the US Code of 2011. This mapping is derived from the text and the structure 
    of the statutes.


### US

The processing for the US Code is executed in multiple steps:


#### 1. Data input

Input is are ZIP files downloaded from the US House of Representatives Office of the Law 
Revision Counsel. We use annual versions in XHTML format that are available on 
https://uscode.house.gov/download/annualhistoricalarchives/downloadxhtml.shtml.
Files should be located at `../legal-networks-data/us/1_input`. 
This folder should contain an unzipped yearly folders.

You can automatically obtain the required data running `download_us_code_data.py`.


#### 2. XML Files

- Files containing titles of the US Code are copies to `temp/us/11_htm`. 
    Appendices and Stylesheets are filtered.
- Simple XML files focusing on the structure are generated from the XHTML files. 
    Results can be found in `temp/us/12_xml`
- Text segments containing a cross-reference are annotated in the XML files. Results are saved to 
    `temp/us/13_reference_areas`.
- The contents of the annotated cross-references are extracted and added to the XML. 

The results of the XML generation are saved to `../legal-networks-data/us/2_xml`.


#### 3. Hierarchy Graphs    

Graphs containing the hierarchical structure of the statutes are saved to `../legal-networks-data/us/3_hierarchy_graph`
in separate files for each Title and annual version.

Hierarchy graphs are avaiable in two resolutions: 
- Sections level
- At least section level, modelling as many elements below as possible.  


#### 4. Crossreference graphs

- Lists of all sections in the US Code at a specific point in time is generated to obtain a list of possible
    destinations of cross-references. This is a perparation to draw edges from the reference to the destination of a
    cross-reference. The lists are stored at `temp/us/31_crossreference_lookup`.
- Lists of all cross-references are generated. They contain the ID of the referencing and referenced element. 
    The lists are located at `temp/us/32_crossreference_edgelist`.
- Hierarchy graphs of the individual Titles are combined and edges for cross-references are added within and between 
    Titles.

One graph for each annual version of the US Code is stored at `../legal-networks-data/us/4_crossreference_graph`.


#### 5. Snapshot mapping edgelists

Snapshot mapping edgelists are stored at `../legal-networks-data/us/5_snapshot_mapping_edgelist`.


#### Germany

#### 1. Data input

Input are xml-files in simplified format of https://www.gesetze-im-internet.de.
These files can be generated from two sources:

1. XML-files provided at https://www.gesetze-im-internet.de. To obtain older versions of this website
    use our public archive at https://github.com/legal-networks/gesetze-im-internet. 
    Downloaded files must be simplified before they are suitable input. 
    Use `download_de_gesetze_im_internet_data.py` to download, simplify and rename the source files. 
    This replaces step `prepare_input` in the pipeline. 
    (Make sure that you do not run this step. It is not possible to run `all` steps.)
2. An unpublished juris export can be used to obtain the data.
    Whereas this datasource covers a longer time period, it is not available under a permissive license.

#### 2. XML Files

- Files in the simplified format of Gesetze im Internet are generated and saved to `temp/de/11_gii_xml`
- Simple XML files focusing on the structure are generated from the XML files. 
    Results can be found in `temp/de/12_xml`. 
    - Simultaneously a list of the names of all statutes (Gesetze) is saved to
        `temp/de/12_xml_law_names.csv` with a mapping to the corresponding files. 
        This is used to extract cross-references, as statutes are typically referenced by their name. 
        Names are saved in a stemmed version. 
    - Furthermore `temp/de/12_xml_law_names_compiled.pickle` is generated. 
        It contains the same information as `12_xml_law_names.csv`, 
        but it optimized to obtain the stemmed names of all valid law at specific dates.
- Text segments containing a cross-reference are annotated in the XML files. Results are saved to 
    `temp/de/13_reference_areas`.
- The contents of the annotated cross-references are extracted and added to the XML. 

The results of the XML generation are saved to `../legal-networks-data/de/2_xml`.

#### 3. Hierarchy Graphs

Hierarchy Graphs are saved to `../legal-networks-data/de/3_hierarchy_graph`. 
See documentation regarding US hierarchy graphs for further information.

#### 4. Cross-reference graphs

In general cross-reference graphs are generated in the same manner as for the US dataset 
(see above for further information). 
(Interim) results are saved to 
`temp/us/31_crossreference_lookup`, 
`temp/us/32_crossreference_edgelist` and 
`../legal-networks-data/us/4_crossreference_graph` respectively.

A major difference are the possible dates to create cross-reference graphs for. 
For the US annual version are available. The juris export allows to select any day to create a snapshot for.
If you rely on https://github.com/legal-networks/gesetze-im-internet as data source, you can only select days 
for which a snapshots were created.

#### 5. Snapshot mapping edgelists

Snapshot mapping edgelists are stored at `../legal-networks-data/de/5_snapshot_mapping_edgelist`.


