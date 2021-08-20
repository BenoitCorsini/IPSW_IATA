import pandas as pd
import pickle

# %% script to save IOSA registry and airline codes as dataframes

if __name__ == '__main__':


    # %% Load airline IOSA registration status

    file = 'IOSA Registry/IPSW - IOSA Registry.xlsx'
    registry_IOSA = pd.read_excel(file, index_col=0)  

    print(registry_IOSA)

    with open('registry_IOSA.pkl','wb') as file:
        pickle.dump(registry_IOSA,file)

    # %% Load airline information for imputing IATA codes

    file = 'Airport & Airline List/DIMAIRLINE.csv'
    airline_info = pd.read_csv(file) # Initialize dataframe

    print(airline_info)

    with open('airline_info.pkl','wb') as file:
        pickle.dump(airline_info,file)