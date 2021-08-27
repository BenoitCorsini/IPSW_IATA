import pickle
from dask import dataframe as dd
from dask.distributed import Client
import os

from query_schedule import agg_data

def read_csv_CS_files(filename):

    sample_file = 'Schedule_Codeshare/2011_01_06_SCH_CS_JobId1915926.csv'
    text = open(sample_file).readlines()
    titles = text[0].split(sep=',')
    titles[-1] = titles[-1].rstrip()

    dtypes = {}

    for title in titles[0:6]:
        dtypes[title] = 'str'

    dtypes[titles[6]] = 'int'

    for title in titles[7:9]:
        dtypes[title] = 'str'

    for title in titles[9:-1]:
        dtypes[title] = 'int'

    schedule_data = dd.read_csv(filename, delimiter=',', 
        header=0,
        parse_dates=[titles[-1],],
        names=titles,dtype=dtypes,
        blocksize=64000000) # = 64 Mb chunks

    return schedule_data

def read_csv_OP_files(filename):

    sample_file = 'Schedule_Operating/2011_01_06_SCH_OP_JobId1911085.csv'
    text = open(sample_file).readlines()
    titles = text[0].split(sep=',')
    titles[-1] = titles[-1].rstrip()

    dtypes = {}

    for title in titles[0:6]:
        dtypes[title] = 'str'

    dtypes[titles[6]] = 'int'
    dtypes[titles[7]] = 'str'

    for title in titles[8:-1]:
        dtypes[title] = 'int'

    schedule_data = dd.read_csv(filename, delimiter=',', 
        header=0,
        parse_dates=[titles[-1],],
        names=titles,dtype=dtypes,
        blocksize=64000000) # = 64 Mb chunks

    return schedule_data

def check_IOSA(row, process_IOSA):

    time_periods = process_IOSA['RegistrationPeriod'].loc[row['IATA AL']]

    is_iosa = 0
    for period in time_periods:

        if (row['Time series'] >= period[0]) and (row['Time series'] <=period[1]):
            is_iosa=1
            break

    return is_iosa

def check_CS_OP(row, CS=True):

    if CS:
        CS_OP = 1
    else:
        CS_OP = 0

    return CS_OP

def find_dep(row, airport_info, column):
    data = airport_info[column].loc[row['Dep Airport Code']]
    return data

def find_arr(row, airport_info, column):
    data = airport_info[column].loc[row['Arr Airport Code']]
    return data

if __name__ == '__main__': 
    
    client = Client(n_workers=4)

    # Load IOSA registry
    with open("IOSA_info.pkl",'rb') as file:
        process_IOSA = pickle.load(file)

    with open('airport_info.pkl','rb') as file:
        airport_info = pickle.load(file)

    extension = 'csv'
    filenames = os.path.join('Schedule_Codeshare', '*.{}'.format(extension))
    schedule_CS = read_csv_CS_files(filenames)

    schedule_CS = schedule_CS.dropna(subset=['IATA AL'])
    schedule_CS = schedule_CS[schedule_CS['No of Stops']==0]
    schedule_CS = schedule_CS.drop(schedule_CS.columns[7:9], axis=1) # drop columns 7 and 8

    filenames = os.path.join('Schedule_Operating', '*.{}'.format(extension))
    schedule_OP = read_csv_OP_files(filenames)

    schedule_OP = schedule_OP.dropna(subset=['IATA AL'])
    schedule_OP = schedule_OP[schedule_OP['No of Stops']==0]
    schedule_OP = schedule_OP.drop(schedule_OP.columns[7], axis=1) # drop columns 7 and 8

    # Generate CS_OP column
    schedule_CS['Is CS']=schedule_CS.apply(check_CS_OP,CS=True, axis=1, meta=('Is CS', 'int'))
    schedule_OP['Is CS']=schedule_OP.apply(check_CS_OP,CS=False, axis=1, meta=('Is CS', 'int'))
    
    # Merge the two dataframe together
    schedule_data = schedule_CS.append(schedule_OP)

    # pick only airlines in passenger dataset with entries in IOSA registry and airport list
    schedule_data = schedule_data[schedule_data['IATA AL'].isin(process_IOSA['IATA Code'])]
    schedule_data = schedule_data[schedule_data['Dep Airport Code'].isin(airport_info['CD_LOCATIONIATA'])]
    schedule_data = schedule_data[schedule_data['Arr Airport Code'].isin(airport_info['CD_LOCATIONIATA'])]

    # Identify IOSA registered flights
    process_IOSA_noindex = process_IOSA.set_index('IATA Code')
    schedule_data['Is IOSA'] = schedule_data.apply(check_IOSA, process_IOSA=process_IOSA_noindex, axis=1, meta=('Is IOSA', 'int'))

    # Identify all airport information
    airport_info_noindex = airport_info.set_index('CD_LOCATIONIATA')
    schedule_data['dep_LATITUDE'] = schedule_data.apply(find_dep, airport_info=airport_info_noindex, column='NO_LATITUDE', axis=1, meta=('dep_LATITUDE', 'float64'))
    schedule_data['dep_LONGITUDE'] = schedule_data.apply(find_dep, airport_info=airport_info_noindex, column='NO_LONGITUDE', axis=1, meta=('dep_LONGITUDE', 'float64'))
    schedule_data['dep_COUNTRY'] = schedule_data.apply(find_dep, airport_info=airport_info_noindex, column='NK_ISOALPHA2COUNTRY', axis=1, meta=('dep_COUNTRY', 'str'))
    schedule_data['dep_REGION'] = schedule_data.apply(find_dep, airport_info=airport_info_noindex, column='NM_REGIONIATA', axis=1, meta=('dep_REGION', 'str'))

    schedule_data['arr_LATITUDE'] = schedule_data.apply(find_arr, airport_info=airport_info_noindex, column='NO_LATITUDE', axis=1, meta=('arr_LATITUDE', 'float64'))
    schedule_data['arr_LONGITUDE'] = schedule_data.apply(find_arr, airport_info=airport_info_noindex, column='NO_LONGITUDE', axis=1, meta=('arr_LONGITUDE', 'float64'))
    schedule_data['arr_COUNTRY'] = schedule_data.apply(find_arr, airport_info=airport_info_noindex, column='NK_ISOALPHA2COUNTRY', axis=1, meta=('arr_COUNTRY', 'str'))
    schedule_data['arr_REGION'] = schedule_data.apply(find_arr, airport_info=airport_info_noindex, column='NM_REGIONIATA', axis=1, meta=('arr_REGION', 'str'))

    schedule_data = schedule_data.compute()
    with open('Schedule.pkl','wb') as file:
        pickle.dump(schedule_data,file)

    schedule_data_agg = agg_data(schedule_data)
    with open('Schedule_agg.pkl','wb') as file:
        pickle.dump(schedule_data_agg,file)

    # Generate sample data
    sample_data = schedule_data.sample(frac=0.0004)
    with open('Schedule_sample.pkl','wb') as file:
        pickle.dump(sample_data,file)

    sample_data.to_excel("Schedule_sample.xlsx")
    sample_data.to_csv("Schedule_sample.csv")