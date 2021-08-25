import pandas as pd
import glob
import pickle
from dask import dataframe as dd
from dask.distributed import Client
import os

import datetime

'''
script to load all Passenger data files. 
Files are extremely large reccommend using dask and reducing data
'''

def read_tsv_files(filename):

    sample_file = 'passenger/2017_01_Dynamic_Table_Estimate_756889.tsv'
    text = open(sample_file).readlines()
    titles = text[5].split(sep='\t')
    titles[-1] = titles[-1].rstrip()

    dtypes = {}

    for title in titles[1:-2]:
        dtypes[title] = 'str'

    dtypes[titles[-2]] = 'float64'
    dtypes[titles[-1]] = 'float64'

    passenger_data = dd.read_csv(filename, delimiter='\t', 
        skiprows=6, skipfooter = 12,
        parse_dates=[titles[0],],
        names=titles,dtype=dtypes,
        blocksize=64000000) # = 64 Mb chunks

    passenger_data = passenger_data.dropna(subset=['Seg Mkt Airline Code'])

    # dask operation
    passenger_data = passenger_data.drop(passenger_data.columns[1:13], axis=1)

    return passenger_data

def check_IOSA(row, process_IOSA):

    time_periods = process_IOSA['RegistrationPeriod'].loc[row['Seg Mkt Airline Code']]

    is_iosa = False
    for period in time_periods:

        if (row['Travel Month'] >= period[0]) and (row['Travel Month'] <=period[1]):
            is_iosa=True
            break

    return is_iosa

if __name__ == '__main__':

    
    client = Client(n_workers=4)

    store = pd.HDFStore('./passenger_dataset.h5') # Hardisk file for storing data

    extension = 'tsv'
    filenames = os.path.join('Passenger', '*.{}'.format(extension))

    # Load IOSA registry
    with open("IOSA_info.pkl",'rb') as file:
        process_IOSA = pickle.load(file)

    passenger_data = read_tsv_files(filenames)
    # passenger_data = passenger_data.head(500)

    # Check which flights are codeshared
    is_codeshared = passenger_data['Seg Mkt Airline Code'] != passenger_data['Seg Op Al Code']
    passenger_data['is CS'] = is_codeshared

    # pick only airlines in passenger dataset with entries in IOSA registry
    passenger_data = passenger_data[passenger_data['Seg Mkt Airline Code'].isin(process_IOSA['IATA Code'])]
    study_start = pd.Timestamp('2017-1-1')
    study_end = pd.Timestamp('2021-5-1')

    flight_month = passenger_data['Travel Month']
    flight_code = passenger_data['Seg Mkt Airline Code']
    
    # Identify IOSA registered flights
    process_IOSA_noindex = process_IOSA.set_index('IATA Code')
    passenger_data['Is IOSA'] = passenger_data.apply(check_IOSA, process_IOSA=process_IOSA_noindex, axis=1, meta=('Is IOSA', 'bool'))

    sample_data = passenger_data.sample(frac=0.0004).compute()

    with open('Passenger.pkl','wb') as file:
        pickle.dump(sample_data,file)

    sample_data.to_excel("Passenger.xlsx")
    sample_data.to_csv("Passenger.csv")