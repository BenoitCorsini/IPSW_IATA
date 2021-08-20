import pandas as pd
import glob
import pickle

# %% script to load all Schedule_Codeshare files into a single dataframe 

if __name__ == '__main__':

    extension = 'csv'
    result = glob.glob('Schedule_Codeshare/*.{}'.format(extension))

    schedule_CS = pd.read_csv(result[0]) # Initialize dataframe
    
    i = 1
    for file in result[1:]: # concatenate all dataframes together 
        print('processed %i of %i files' %(i,len(result)))
        x = pd.read_csv(file)
        schedule_CS = schedule_CS.append(x)
        i += 1

    schedule_CS.dropna(inplace=True)

    print(schedule_CS)
    grouped_schedule_CS = schedule_CS.groupby(['IATA AL']) # group fields by airline

    with open('schedule_CS.pkl','wb') as file:
        pickle.dump(schedule_CS,file)

    with open('grouped_schedule_CS.pkl','wb') as file:
        pickle.dump(grouped_schedule_CS,file)