import pandas as pd
import glob
import pickle
import datetime as dt

if __name__ == '__main__':

    with open('grouped_schedule_CS.pkl','rb') as file:
        grouped_schedule_CS = pickle.load(file)

    keys = []
    for key, group in grouped_schedule_CS:
        keys += [key]

    with open("IOSA_info.pkl",'rb') as file:
        process_IOSA = pickle.load(file)

    study_start = pd.Timestamp('2011-1-1')
    study_end = pd.Timestamp('2021-6-1')

    ndays_study = (study_end - study_start).days + 1

    # Initialize empty dataframe
    stats_IOSA = pd.DataFrame(columns=['Airline','IATA Code','IOSA flights','Non IOSA flights','ASMs IOSA','ASMs non IOSA','Frequency IOSA','Frequency non IOSA'])

    i = 0
    for index, row in process_IOSA.iterrows():
        print(row['IATA Code'])

        if row['IATA Code'] in keys:

            airline = grouped_schedule_CS.get_group(row['IATA Code'])

            # Identify IOSA registered flights
            mask_study = (airline['Time series'] >= str(study_start)) & (airline['Time series'] <= str(study_end))

            for period in row['RegistrationPeriod']:
                
                mask = (airline['Time series'] >= str(period[0])) & (airline['Time series'] <= str(period[1]))
                mask_IOSA = mask_study & mask
                mask_nonIOSA = mask_study & ~mask

            count_IOSA = len(airline.loc[mask_IOSA])
            asm_IOSA = sum(airline.loc[mask_IOSA]['ASMs'])
            freq_IOSA = sum(airline.loc[mask_IOSA]['Frequency'])

            count_nonIOSA = len(airline.loc[mask_nonIOSA])
            asm_nonIOSA = sum(airline.loc[mask_nonIOSA]['ASMs'])
            freq_nonIOSA = sum(airline.loc[mask_nonIOSA]['Frequency'])

            n_days_IOSA = row['number of days']
            n_days_non_IOSA = ndays_study - n_days_IOSA

            # Normalize by number of days 

            # non IOSA flights
            if n_days_non_IOSA == 0:
                count_nonIOSA = 0
                asm_nonIOSA = 0
                freq_nonIOSA = 0
            else:
                count_nonIOSA /= n_days_non_IOSA
                asm_nonIOSA /= n_days_non_IOSA
                freq_nonIOSA /= n_days_non_IOSA

            # IOSA flights
            if n_days_IOSA == 0:
                count_IOSA = 0
                asm_IOSA = 0
                freq_IOSA = 0
            else:
                count_IOSA /= n_days_IOSA
                asm_IOSA /= n_days_IOSA
                freq_IOSA /= n_days_IOSA

            x = pd.Series(data={'Airline':row['Airline'], 'IATA Code': row['IATA Code'], 
                'IOSA flights': count_IOSA, 'Non IOSA flights': count_nonIOSA,
                'ASMs IOSA': asm_IOSA, 'ASMs non IOSA': asm_nonIOSA, 
                'Frequency IOSA': freq_IOSA, 'Frequency non IOSA': freq_nonIOSA }, name=i)

            stats_IOSA = stats_IOSA.append(x)

            i += 1

    stats_IOSA.to_excel("stats_IOSA.xlsx")

    with open('stats_IOSA.pkl','wb') as file:
        pickle.dump(stats_IOSA,file)

        