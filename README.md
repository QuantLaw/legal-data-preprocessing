# legal-data-preprocessing

## Statutes

US and German federal statutes are converted from official sources (or juris) 
to multiple clean formats focussing on the structure of the law.

Output formats are:

- XML files containing the text, the hierarchical structure of the law and cross-references.
- GraphML files containing the hierarchical structure of the statutes.
- GraphML files containing the hierarchical structure of the statutes 
    and the cross-references between different elements of the statutes.
- Snapshot mapping edgelists: These lists map elements of a network at one point in time 
    to a snapshot at another time. It encodes e.g. where a clause of the US Code in 2010 is 
    located in the US Code of 2011. This mapping is derived from the text and the structure 
    of the statutes.


### US

The processing for the US Code is executed in multiple steps:


#### 1. Data input

Input is are ZIP files downloaded from the US House of Representatives Office of the Law 
Revision Counsel. We use annual versions in XHTML format that are available on 
https://uscode.house.gov/download/annualhistoricalarchives/downloadxhtml.shtml.
Files should be located at `../legal-networks-data/us/1_downloads`. 
This folder should contain an unzipped yearly folders.


#### 2. XML Files

- Files containing titles of the US Code are copies to `temp/us/11_htm`. 
    Appendices and Stylesheets are filtered.
- Simple XML files focussing on the structure are generated from the XHTML files. 
    Results can be found in `temp/us/12_xml`
- Text segments containing a cross-reference are annotated in the XML files. Results are saved to 
    `temp/us/13_reference_areas`.
- The c of the annotated cross-references are extracted and added to the XML. 

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

#### 2. XML Files

#### 3. Hierarchy Graphs    

#### 4. Crossreference graphs

#### 5. Snapshot mapping edgelists

Snapshot mapping edgelists are stored at `../legal-networks-data/de/5_snapshot_mapping_edgelist`.
