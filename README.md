# IATA data mining scripts
<a name="top"></a>

The scripts in this version of the repository are for processing the raw IATA data provided for the IPSW workshop.

# Setup

I recommend using the Anaconda distribution of python 3 with Visual Studio Code as the IDE. Must install the python plug-in [https://code.visualstudio.com/docs/languages/python](https://code.visualstudio.com/docs/languages/python) after installing Anaconda.

## Dependencies 
[![Generic badge](https://img.shields.io/badge/Python-3.8.3-<COLOR>.svg)](https://anaconda.org/)

- **Python** [3.8.3](https://anaconda.org/) or newer for statistics and post-processing
	- **pandas** [1.2.4](https://pandas.pydata.org/)
	- **numpy** [1.20.2](https://pandas.pydata.org/)
	
## Directory structure

Organize the downloaded data files as follows:

```
IPSW_IATA/
├── Airport & Airline List/
│   ├── DIMAIRLINE.csv
│   └── DIMLOCATION.csv
├── IOSA Registry/
│   └── IPSW - IOSA Registry.xlsx
├── Passenger/
│   ├── 2017_01_Dynamic_Table_Estimate_756889.tsv
│   |		|
│   |		|
│   |		|
│   └──22021_05_Dynamic_Table_Estimate_768042.tsv
├── Schedule_Codeshare/
│   ├── 2011_01_06_SCH_CS_JobId1915926.csv
│   |		|
│   |		|
│   |		|
│   └──2021_01_06_SCH_CS_JobId1918716.csv
└── Schedule_Operating/
    ├── 2011_01_06_SCH_OP_JobId1911085.csv
    |		|
    |		|
    |		|
    └──2021_07_12_SCH_OP_JobId1911803.csv
```

# Example usage

The supplied python scripts are used to organize the raw data and prepare for post processing and statistical analysis in Python. The scripts should be run in this order:

- [load_data_CS.py](load_data_CS.py): Loads all the csv files under the [Schedule_Codeshare](Schedule_Codeshare) directory and saves it as a pandas dataframe in a pickle file [schedule_CS.pkl](schedule_CS.pkl). Also groups data by airline (the column `IATA AL`) and saves the grouped data into [grouped_schedule_CS.pkl](grouped_schedule_CS.pkl)
- [load_data_IOSA.py](load_data_IOSA.py): Loads all the excel files under the [IOSA Registry](IOSA Registry) and [Airport & Airline List](Airport & Airline List) directories and saves them as pandas dataframes in pickle files [IOSA_info.pkl](IOSA_info.pkl) and [airline_info.pkl](airline_info.pkl), respectively.
- [process_data_IOSA.py](process_data_IOSA.py): Loads the results of the [load_data_IOSA.py](load_data_IOSA.py) script and creates the files [processed_IOSA.xlsx](processed_IOSA.xlsx) and [IOSA_info.pkl](IOSA_info.pkl) which contain the exact timelines during which each airline was IOSA registered.
- [process_data_CS.py](process_data_CS.py): Loads the results of all the previous scripts and outputs interesting statistics about the effect of IOSA registration on flight frequency and Available Seat Miles (ASMs) in [stats_IOSA.xlsx](stats_IOSA.xlsx).

More detailed analysis and data visualization will be conducted during the workshop and will be added to this repository.

# Problem statement

- [Airline Codeshare correlative analysis](http://crm.umontreal.ca/probindustrielsEn2021/index.php/iata-eng/)

<!-- <img src="images/Logo_McGill.jpg" width="480"> -->
