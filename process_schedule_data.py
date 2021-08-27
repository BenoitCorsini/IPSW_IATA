import pickle
from dask import dataframe as dd
from dask.distributed import Client
import os
import pandas as pd

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

def agg_data(schedule_data):
    
    # To combine combination of pairs of airports
    sorted_airports = schedule_data[['Dep Airport Code', 'Arr Airport Code']].apply(sorted, axis=1)
    schedule_data[['Dep Airport Code', 'Arr Airport Code']] = pd.DataFrame(sorted_airports.to_list(), 
        columns=['Dep Airport Code', 'Arr Airport Code'], index= schedule_data.index)

    # Groupby airport pair and year
    group_schedule_data = schedule_data.groupby( [pd.Grouper(key='Time series', freq=('1Y')),'Dep Airport Code', 'Arr Airport Code'])

    # # Groupby airport pair and year
    # group_schedule_data = schedule_data.groupby( ['dep_COUNTRY',])

    # Initialize empty dataframe
    columns = ['Dep Airport Code',
                'Arr Airport Code',
                'dep_LATITUDE',
                'dep_LONGITUDE',
                'dep_COUNTRY',
                'dep_REGION',
                'arr_LATITUDE',
                'arr_LONGITUDE',
                'arr_COUNTRY',
                'arr_REGION',]

    columnsAgg = ['year',
                  'International',
                  'Domestic',
                  'IOSA CS',
                  'IOSA OP',
                  'nonIOSA CS',
                  'nonIOSA OP',
                  'avg Frequency',
                  'avg Seats (Total)',
                  'avg ASMs',]

    columnsOrg = ['year',
                  'Is International',
                  'Is Domestic',
                  'Is IOSA CS',
                  'Is IOSA OP',
                  'Is nonIOSA CS',
                  'Is nonIOSA OP',
                  'Frequency',
                  'Seats (Total)',
                  'ASMs']

    airline_data_agg = pd.DataFrame(columns=columns+columnsAgg)

    airline_data_agg = group_schedule_data[columns[0]].first().to_frame().reset_index(drop=True)

    for column in columns:
        series = group_schedule_data[column].first().to_frame().reset_index(drop=True)
        airline_data_agg = pd.concat([airline_data_agg, series], axis=1)
    
    series = group_schedule_data['Time series'].first().to_frame().reset_index(drop=True)
    series['Time series'] = series['Time series'].dt.year
    series.rename(columns={'Time series':'year'}, inplace=True)
    airline_data_agg = pd.concat([airline_data_agg, series], axis=1)

    for col,col_o in zip(columnsAgg[1:7],columnsOrg[1:7]):
        series = group_schedule_data[col_o].sum().to_frame().reset_index(drop=True)
        series.rename(columns={col_o : col}, inplace=True)
        airline_data_agg = pd.concat([airline_data_agg, series], axis=1)

    for col,col_o in zip(columnsAgg[7:],columnsOrg[7:]):
        series = group_schedule_data[col_o].mean().to_frame().reset_index(drop=True)
        series.rename(columns={col_o : col}, inplace=True)
        airline_data_agg = pd.concat([airline_data_agg, series], axis=1)

    return airline_data_agg

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

    # Generate combination of CS OP IOSA and nonIOSA
    schedule_data['Is IOSA CS'] = ((schedule_data['Is IOSA'] == 1) & (schedule_data['Is CS'] == 1)).astype(int)
    schedule_data['Is IOSA OP'] = ((schedule_data['Is IOSA'] == 1) & (schedule_data['Is CS'] == 0)).astype(int)
    schedule_data['Is nonIOSA CS'] = ((schedule_data['Is IOSA'] == 0) & (schedule_data['Is CS'] == 1)).astype(int)
    schedule_data['Is nonIOSA OP'] = ((schedule_data['Is IOSA'] == 0) & (schedule_data['Is CS'] == 0)).astype(int)

    # Generate is internation domestic columns
    schedule_data['Is International'] = (schedule_data['International/Domestic'] == 'International').astype(int)
    schedule_data['Is Domestic'] = (schedule_data['International/Domestic'] == 'Domestic').astype(int)

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