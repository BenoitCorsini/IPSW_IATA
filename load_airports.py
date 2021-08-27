import pickle
import pandas as pd

if __name__ == '__main__': 

    # %% Load airline information for imputing IATA codes

    file = 'Airport & Airline List/DIMLOCATION.csv'
    airport_info = pd.read_csv(file) # Initialize dataframe

    airport_info.dropna(subset=['CD_LOCATIONIATA','NO_LATITUDE','NO_LONGITUDE'], inplace=True)
    airport_info.drop_duplicates(subset=['CD_LOCATIONIATA'], keep='first', inplace=True, ignore_index=False)

    airport_info = airport_info[airport_info['NM_LOCATIONTYPE'] == 'Airport']

    with open('airport_info.pkl','wb') as file:
        pickle.dump(airport_info,file)

    airport_info.to_excel("airport_info.xlsx")
    airport_info.to_csv("airport_info.csv")