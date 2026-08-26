### What Seattle Read During Covid: A longitudinal analysis of Seattle Public Library borrowing patterns during and after the Covid-19 pandemic (2018-2025).

#### Links
- [Paper](https://github.com/staciefase/what-seattle-read-during-covid/blob/main/What%20Seattle%20Read%20During%20Covid.pdf)
- [Jupyter Notebook](https://github.com/staciefase/what-seattle-read-during-covid/blob/main/What%20Seattle%20Read%20During%20Covid.ipynb)

#### Overview

During the Covid-19 pandemic, libraries faced unprecedented disruptions. Physical spaces closed, digital services expanded, and many aspects of daily life moved online. This project uses public Seattle Public Library circulation data to examine how borrowing behavior changed before, during, and after the pandemic. Using more than 51 million checkout records from the Seattle Open Data portal, I analyzed borrowing trends across multiple dimensions, including:

- Borrowing format (print books, eBooks, audiobooks, etc.)
- Subject combinations and genres
- Selected thematic categories, including LGBTQIA+ and African American-related materials

The central question of the project was: **Did the Covid-19 pandemic change what people borrowed, how they borrowed it, or both?**

#### Key Findings
**People changed how they borrowed more than what they borrowed**

The strongest pattern in the dataset is a shift from physical to digital borrowing.

- Before Covid, physical books represented the largest share of circulation.
- During Covid, ebook borrowing surged and became the dominant format.
- Audiobook borrowing increased substantially.
- Even after recovery, digital borrowing remained elevated relative to pre-pandemic levels.

This suggests that Covid accelerated an existing transition toward digital borrowing rather than creating a temporary disruption that later reversed.

<img width="1790" height="789" alt="output_31_0" src="https://github.com/user-attachments/assets/b7e6d3a2-bd2d-421a-9f3d-43ef8b124c52" />

*Note: There was a ransomware attack in Spring 2024 that affected checkout data, giving only partial numbers. For the purposes of this project, I NULL'd out the values, hence the dip in activity (further discussed in the Limitations section below*

<img width="1780" height="985" alt="output_33_0" src="https://github.com/user-attachments/assets/d0ed11e5-d2da-4bf7-a60a-bacb6d5055c5" />
<img width="1780" height="985" alt="output_36_0" src="https://github.com/user-attachments/assets/4aba79f8-136f-40a4-8a80-b19c68e392c1" />

**Broad reading interests remained surprisingly stable**

Despite dramatic changes in borrowing format, the most frequently borrowed subject combinations remained relatively consistent throughout the analysis period.

Examples include:

- Fiction, Literature
- Fiction, Mystery
- Fiction, Romance
- Fiction, Mystery, Suspense, Thriller
- Juvenile Fiction

This suggests that patrons largely continued reading the same types of material even as they changed the formats through which they accessed them.

<img width="1790" height="1279" alt="output_44_0" src="https://github.com/user-attachments/assets/c976f59b-1f90-4cc6-a134-7b5bb1a94b4d" />

**LGBTQIA+-related materials exhibited sustained growth**

A targeted thematic analysis revealed that:

- African American-related materials experienced a temporary increase during the Covid period before returning closer to baseline levels.
- LGBTQIA+-related materials showed sustained growth throughout the Recovery and Post-Covid periods.
- The average monthly share of circulation associated with LGBTQIA+-related materials increased from approximately 0.57% before Covid to 2.94% in the Post-Covid period.

<img width="990" height="590" alt="output_48_0" src="https://github.com/user-attachments/assets/2fa7de10-f6bd-4e8c-bd9c-ac500bb03994" />  
<img width="1023" height="645" alt="output_53_0" src="https://github.com/user-attachments/assets/f0c9dc09-93ae-4498-a19d-d5abe8d936a4" />

#### Data Source
This project uses publicly available data from the Seattle Open Data portal: [Seattle Public Library Checkouts by Title Dataset](https://data.seattle.gov/Community-and-Culture/Checkouts-by-Title/tmmm-ytt6/explore)

The dataset includes:

- Checkout counts by month
- Titles and ISBNs
- Material types
- Publishers and creators
- Catalog subject metadata

As of July 2026, the dataset contained over 51 million records.

#### Technical Approach
The dataset was too large to comfortably analyze as flat CSV files, so I implemented a simple analytical data pipeline.

**Bronze Layer**

Raw yearly downloads from the Seattle Open Data API stored as Parquet files.

**Silver Layer**

Standardized and cleaned Parquet files with:

- consistent datatypes
- schema normalization
- handling for missing fields

**Gold Layer**

Aggregated analytical datasets used for visualization and reporting.

**Tools Used** 
- Python
- DuckDB
- Pandas
- Apache Parquet
- Matplotlib
- Seaborn
- Seattle Open Data / Socrata API

#### Limitations

Several important caveats apply:

- Subject metadata consists of concatenated catalog values rather than a standardized genre taxonomy.
- Cataloging practices appear to have evolved over time.
- Theme analyses rely on keyword matching.
- Checkout activity reflects utilization of available resources, not necessarily total demand.
- A ransomware incident in 2024 resulted in gaps in portions of the circulation data.

See the paper for a complete discussion of limitations and methodology.

#### Future Areas of Focus
Potential extensions include:

- Developing a normalized subject taxonomy
- Additional theme analyses (Mental Health, Finance, Technology, Climate, etc.)
- Title-level trend analysis
- Inventory-aware checkout analysis
- Expansion of the study period to the full 2006-2026 dataset
- Examination of other major disruptions, including the 2008 financial crisis

#### Why This Project?
I started this project because I was interested in whether a city's borrowing habits could reveal anything about how a community responds to disruption. The answer turned out to be both simpler and more interesting than I expected: **During Covid, Seattle readers largely continued reading the same kinds of things they had always read, but they fundamentally changed how they accessed them.** That shift from physical to digital borrowing remains visible years after the pandemic and provides a unique glimpse into how public library services adapted during a period of rapid social change.
The paper is available in the repo in PDF format, and the code is available in both .md and .ipynb formats.

#### Contact Information
You can contact me, Stacey Mahuna, at smahuna@gmail.com.

