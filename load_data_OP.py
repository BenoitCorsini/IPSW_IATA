import pandas as pd
import glob
import pickle

# %% script to load all Schedule_Codeshare files into a single dataframe 

if __name__ == '__main__':

    extension = 'csv'
    result = glob.glob('Schedule_Operating/*.{}'.format(extension))

    schedule_OP = pd.read_csv(result[0]) # Initialize dataframe
    
    i = 1
    for file in result[1:]: # concatenate all dataframes together 
        print('processed %i of %i files' %(i,len(result)))
        x = pd.read_csv(file)
        schedule_OP = schedule_OP.append(x)
        i += 1

    schedule_OP.dropna(inplace=True)

    print(schedule_OP)
    grouped_schedule_OP = schedule_OP.groupby(['IATA AL']) # group fields by airline

    with open('schedule_OP.pkl','wb') as file:
        pickle.dump(schedule_OP,file)

    with open('grouped_schedule_OP.pkl','wb') as file:
        pickle.dump(grouped_schedule_OP,file)