import pandas as pd
import glob
import pickle
from dask import dataframe as dd
from dask.distributed import Client

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

    passenger_data = dd.read_csv(filename)

    passenger_data = dd.read_csv(filename, delimiter='\t', 
        skiprows=6, skipfooter = 12,
        parse_dates=[titles[0],],
        names=titles,dtype=dtypes,
        blocksize=64000000) # = 64 Mb chunks

    return passenger_data

if __name__ == '__main__':

    
    client = Client(n_workers=2)

    store = pd.HDFStore('./passenger_dataset.h5') # Hardisk file for storing data

    extension = 'tsv'
    filenames = os.path.join('Passenger', '*.{}'.format(extension))

    # Load IOSA registry
    with open("IOSA_info.pkl",'rb') as file:
        process_IOSA = pickle.load(file)

    passenger_data = read_tsv_files(filenames)

    # store.put('passenger_data',
    #        passenger_data,
    #        format='table',
    #        data_columns=True)






#############################################################
# Work in progress

    # study_start = pd.Timestamp('2017-1-1')
    # study_end = pd.Timestamp('2021-5-1')

    # flight_month = passenger_data['Travel Month'].head(20)
    # flight_code = passenger_data['Seg Mkt Airline Code'].head(20)

    # # Identify IOSA registered flights
    # mask_study = (flight_month >= str(study_start)) & (flight_month <= str(study_end))

    # process_IOSA = process_IOSA.set_index('IATA Code')
    # time_periods = process_IOSA['RegistrationPeriod'].reindex(flight_code.values)

    
    # df_lookup.loc[df.index.hour].values

    # grouped_passenger_data = passenger_data.groupby(['Seg Mkt Airline Code']) # group fields by airline
    # airline = grouped_passenger_data.get_group('HZ')

    # df = passenger_data.tail(20)
    # print(df)

    # df = dd.from_pandas(passenger_data, npartitions=1)
    # df.compute()


# def check_IOSA(df):

#     study_start = pd.Timestamp('2017-1-1')
#     study_end = pd.Timestamp('2021-5-1')

#     flight_month = df['Travel Month'].compute()
#     flight_code = df['Seg Mkt Airline Code'].compute()

#     # Identify IOSA registered flights
#     mask_study = (flight_month >= str(study_start)) & (flight_month <= str(study_end))

#     # Load IOSA registry
#     with open("IOSA_info.pkl",'rb') as file:
#         process_IOSA = pickle.load(file)

#     row = process_IOSA.loc[process_IOSA['column_name'] == flight_code]

#     for period in row['RegistrationPeriod']:
        
#         mask = (airline_month >= str(period[0])) & (airline_month <= str(period[1]))
#         mask_IOSA = mask_study & mask
#         mask_nonIOSA = mask_study & ~mask

#     count_IOSA = len(airline.loc[mask_IOSA])
#     asm_IOSA = sum(airline.loc[mask_IOSA]['ASMs'])
#     freq_IOSA = sum(airline.loc[mask_IOSA]['Frequency'])

#     count_nonIOSA = len(airline.loc[mask_nonIOSA])
#     asm_nonIOSA = sum(airline.loc[mask_nonIOSA]['ASMs'])
#     freq_nonIOSA = sum(airline.loc[mask_nonIOSA]['Frequency'])

#     return len(series) - series.count()