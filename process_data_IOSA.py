from numpy import isnan
import pandas as pd
import pickle
import datetime as dt

# %% Process IOSA registry and calculate IOSA registration duration

if __name__ == '__main__':

    with open('registry_IOSA.pkl','rb') as file:
        registry_IOSA = pickle.load(file)

    with open('airline_info.pkl','rb') as file:
        airline_info = pickle.load(file)

    print(registry_IOSA)
    print(airline_info)

    grouped_airlines = registry_IOSA.groupby('Airline')
    unique_ICAO = grouped_airlines['ICAO'].unique()
    unique_ICAO.to_excel("ICAO_codes.xlsx")

    # Initialize empty dataframe to store date in
    process_IOSA = pd.DataFrame(columns=['Airline','IATA Code','ICAO','RegistrationPeriod','number of days'])

    i = 0
    for key, group in grouped_airlines:

        print('processing %s' %(key))

        #====================================================#
        # Drop missing fields and impute missing data

        unique_ICAO = group['ICAO'].unique()
        unique_ICAO = [x for x in unique_ICAO if str(x) != 'nan'] # drop NaNs

        # Impute missing IOSA registration dates
        group.dropna(subset = ['RegistrationExpiry'], inplace=True) # remove empty registration entries
        group = group.sort_values(by=['RegistrationExpiry'])

        if len(group) == 0:
            continue # Skip airlines that have no fields left after dropping empty cells
        
        group['Date of Audit Closure'].fillna(group['RegistrationExpiry'] - dt.timedelta(days=720), inplace=True) # impute missing audits

        # List of time periods
        time_periods = []

        # iterate through each row and select 
        open_date = group.iloc[0]['Date of Audit Closure']
        close_date = group.iloc[0]['RegistrationExpiry']

        for index, row in group.iterrows():

            # Overlap detection
            if row['Date of Audit Closure'] - close_date <= dt.timedelta(days=0):
                close_date = row['RegistrationExpiry']
            else:
                time_periods += [(open_date,close_date)]

                open_date = row['Date of Audit Closure']
                close_date = row['RegistrationExpiry']
            
        time_periods += [(open_date,close_date)]

        Registration = time_periods

        #====================================================#
        # Find number of days of IOSA registration

        # Dates over which flights are examined
        study_start = pd.Timestamp('2011-1-1')
        study_end = pd.Timestamp('2021-6-1')

        n_days = 0
        for period in time_periods:
            latest_start = max(study_start, period[0])
            earliest_end = min(study_end, period[1])
            delta = (earliest_end - latest_start).days + 1
            overlap = max(0, delta)

            n_days += overlap

        #====================================================#
        # Lookup IATA code using ICAO code
        for code in unique_ICAO:
            if len(code) == 3:
                codes_IATA = airline_info.loc[airline_info['CD_ICAO'] == code]['CD_IATA'].unique()
                unique_IATA = [x for x in codes_IATA if str(x) != 'nan'] # drop NaNs
                if len (unique_IATA) > 0:
                    code_IATA = unique_IATA[0]
                    code_ICAO = code
                    break
        else:
            # skip loop and do not add this airline to the dataframe
            continue

        x = pd.Series(data={'Airline':key, 'IATA Code': code_IATA, 'ICAO': code_ICAO, 'RegistrationPeriod':Registration, 'number of days':n_days}, name=i)

        process_IOSA = process_IOSA.append(x)
        i += 1

    # duplicate_cond = process_IOSA.duplicated(subset=['IATA Code'])
    # duplicates = process_IOSA[process_IOSA.duplicated(subset=['IATA Code'],keep=False)].sort_values(by=['IATA Code'])

    ##############################################################
    # Merge duplicates

    grouped_duplicates = process_IOSA.groupby('IATA Code')

    # Initialize empty dataframe to store date in
    unique_IOSA = pd.DataFrame(columns=['Airline','IATA Code','ICAO','RegistrationPeriod','number of days'])

    i = 0
    for key, group in grouped_duplicates:
        
        #====================================================#
        # List of time periods
        time_periods = []

        # loop over rows in duplicates and merge timeperiod list
        for index, row in group.iterrows():
            time_periods += row['RegistrationPeriod']

        #====================================================#
        # Merge different time periods together in duplicates

        # Sort period according to open date
        time_periods_sorted = sorted(time_periods, key=lambda x: x[0])

        merged_periods = []

        # iterate through each row and select 
        open_date = time_periods_sorted[0][0]
        close_date = time_periods_sorted[0][1]

        for period in time_periods:

            # Overlap detection
            if period[0] - close_date <= dt.timedelta(days=0):
                close_date = period[1]
            else:
                merged_periods += [(open_date,close_date)]

                open_date = period[0]
                close_date = period[1]
        
        merged_periods += [(open_date,close_date)]

        Registration = time_periods

        # Dates over which flights are examined
        study_start = pd.Timestamp('2011-1-1')
        study_end = pd.Timestamp('2021-6-1')

        #====================================================#
        # Find number of days of IOSA registration
        n_days = 0
        for period in merged_periods:
            latest_start = max(study_start, period[0])
            earliest_end = min(study_end, period[1])
            delta = (earliest_end - latest_start).days + 1
            overlap = max(0, delta)

            n_days += overlap

        #====================================================#
        # Find unique ICAO and airline names
        unique_airline = group['Airline'].unique()
        unique_airline = [x for x in unique_airline if str(x) != 'nan'] # drop NaNs
        Airline = unique_airline[0]

        unique_ICAO = group['ICAO'].unique()
        unique_ICAO = [x for x in unique_ICAO if str(x) != 'nan'] # drop NaNs
        code_ICAO = unique_ICAO[0]

        # Append to dataframe
        x = pd.Series(data={'Airline':Airline, 'IATA Code': key, 'ICAO': code_ICAO, 'RegistrationPeriod':Registration, 'number of days':n_days}, name=i)

        unique_IOSA = unique_IOSA.append(x)
        i += 1

    # # Check for duplicates
    # duplicates = unique_IOSA[unique_IOSA.duplicated(subset=['IATA Code'],keep=False)].sort_values(by=['IATA Code'])
    
    unique_IOSA.to_excel("processed_IOSA.xlsx")
    unique_IOSA.to_csv("processed_IOSA.csv")

    with open('IOSA_info.pkl','wb') as file:
        pickle.dump(unique_IOSA,file)