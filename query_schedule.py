import pickle
from dask import dataframe as dd
from dask.distributed import Client
import pandas as pd

def convert_IOSA(row):

    value = row['Is IOSA']

    if value:
        out = 1
    else:
        out = 0

    return out

def convert_CS_OP(row):

    value = row['CS_OP']

    if value == 'CS':
        out = 1
    elif value == 'OP':
        out = 0

    return out

def save_data(data, dir):

    with open(dir + '.pkl','wb') as file:
        pickle.dump(data,file)

    data.to_excel(dir + ".xlsx")
    data.to_csv(dir + ".csv")

def check_folder(folder):
	import os
	'''check if folder exists, make if not present'''
	if not os.path.exists(folder):
		os.makedirs(folder)

def agg_data_loop(airline_data):
    
    # To combine combination of pairs of airports
    sorted_airports = airline_data[['Dep Airport Code', 'Arr Airport Code']].apply(sorted, axis=1)
    airline_data[['Dep Airport Code', 'Arr Airport Code']] = pd.DataFrame(sorted_airports.to_list(), 
        columns=['Dep Airport Code', 'Arr Airport Code'], index= airline_data.index)

    # Groupby airport pair and year
    group_airline_data = airline_data.groupby( [pd.Grouper(key='Time series', freq=('1Y')),'Dep Airport Code', 'Arr Airport Code'], as_index=False)

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
                'arr_REGION']

    columnsAgg = ['year',
                  '% International',
                  'IOSA CS',
                  'IOSA OP',
                  'nonIOSA CS',
                  'nonIOSA OP',
                  'avg Frequency',
                  'avg Seats (Total)',
                  'avg ASMs',]

    airline_data_agg = pd.DataFrame(columns=columns+columnsAgg)

    keys = []; i = 0
    for key, group in group_airline_data:

        mask_international = (group['International/Domestic'] == 'International')
        mask_IOSA_CS = (group['Is IOSA'] == 1) & (group['Is CS'] == 1)
        mask_IOSA_OP = (group['Is IOSA'] == 1) & (group['Is CS'] == 0)
        mask_nonIOSA_CS = (group['Is IOSA'] == 0) & (group['Is CS'] == 1)
        mask_nonIOSA_OP = (group['Is IOSA'] == 0) & (group['Is CS'] == 0)

        p_international = float(len(group.loc[mask_international]))/len(group)
        n_IOSA_CS = len(group.loc[mask_IOSA_CS])
        n_IOSA_OP = len(group.loc[mask_IOSA_OP])
        n_nonIOSA_CS = len(group.loc[mask_nonIOSA_CS])
        n_nonIOSA_OP = len(group.loc[mask_nonIOSA_OP])

        avgASM = group['ASMs'].mean()
        avgFrequency = group['Frequency'].mean()
        avgSeats = group['Seats (Total)'].mean()

        row = {}

        for column in columns:
            row[column] = group[column].iloc[0]

        row['year'] = group['Time series'].iloc[0].year
        row['% International'] = p_international
        row['IOSA CS'] = n_IOSA_CS
        row['IOSA OP'] = n_IOSA_OP
        row['nonIOSA CS'] = n_nonIOSA_CS
        row['nonIOSA OP'] = n_nonIOSA_OP
        row['avg Frequency'] = avgFrequency
        row['avg Seats (Total)'] = avgSeats
        row['avg ASMs'] = avgASM

        x = pd.Series(data=row, name=i)
        airline_data_agg = airline_data_agg.append(x)

        keys += [key]
        i += 1

    return airline_data_agg

if __name__ == '__main__': 

    with open('Schedule.pkl','rb') as file:
        schedule_data = pickle.load(file)

    with open('airport_info.pkl','rb') as file:
        airport_info = pickle.load(file)
    
    #=============================================================
    # Query by airline
    airline_list = ['A5',]

    for airline in airline_list:
        airline_data = schedule_data[schedule_data['IATA AL'].isin([airline,])].sort_values(by=['Time series'])

        airline_data_agg = agg_data_loop(airline_data)

        path = 'sample_airline/'+airline+'/'
        check_folder(path)
        file = path + 'schedule'
        save_data(airline_data_agg, file)

    