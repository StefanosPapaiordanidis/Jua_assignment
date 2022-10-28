# Jua Assignment

---
Author: Stefanos Papaiordanidis

## Description
This script downloads the ERA5 data from the 
corresponding S3 bucket, and extracts precipitation 
values filtered by date and H3 geospatial 
hierarchical index.  

### Prerequisites
[Anaconda](https://conda.io/projects/conda/en/latest/user-guide/install/download.html)
-or- [Miniconda](https://conda.io/projects/conda/en/latest/user-guide/install/download.html)

`conda env create -f environment.yml`\
`conda activate jua_assignment`

## Running
You can use the default arguments with:

`python main.py create_parquet`

or you can set the ones you prefer like so:\
`python main.py create_parquet timestamp_from 2022-05-05_12:00:00 timestamp_to 2022-05-10_15:00:00 --h3_cell 85754e67fffffff`

The default arguments are:

--timestamp_from 2022-05-01 12:00:00

--timestamp_to 2022-05-02 12:00:00`

--h3_cell 85754e67fffffff

The resulting parquet file will be saved inside the results folder, and it will also be printed as a pandas dataframe.
