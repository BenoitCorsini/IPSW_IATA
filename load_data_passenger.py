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

    
    client = Client(n_workers=8)

    store = pd.HDFStore('./passenger_dataset.h5') # Hardisk file for storing data

    extension = 'tsv'
    filenames = os.path.join('Passenger', '*.{}'.format(extension))
    # filenames = 'passenger/2017_01_Dynamic_Table_Estimate_756889.tsv'

    passenger_data = read_tsv_files(filenames)
    grouped_passenger_data = passenger_data.groupby(['Seg Mkt Airline Code']) # group fields by airline

    airline = grouped_passenger_data.get_group('HZ')

    # df = passenger_data.tail(20)
    # print(df)

    # df = dd.from_pandas(passenger_data, npartitions=1)
    # df.compute()

    # store.put('passenger_data',
    #        passenger_data,
    #        format='table',
    #        data_columns=True)