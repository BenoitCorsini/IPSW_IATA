'''
This script divide csv files after covid in a separate covid_period folder
under Schedule_Codeshare and Schedule_Operating directories 
'''
import pandas as pd
import glob
import os
import shutil

covid_start = pd.Timestamp('2020/3/13')

def eliminate_covid(pathdir,extension):
    
    result = glob.glob(pathdir+'/*.{}'.format(extension))
    
    covid = [x for x in result if "2021" in x or "2020_07" in x]
    
    to_split = [x for x in result if "2020_01" in x][0]
        
    if(extension == "csv"):
        df = pd.read_csv(to_split)
        
        df_filtered = df[df["Time series"] <= str(covid_start)]
        
        df_covid = df[df["Time series"] > str(covid_start)]
        
    df_filtered.to_csv(str(to_split).split(".")[0]+"_pre_covid.csv")
    
    covid_filename = str(to_split).split(".")[0].replace("2020_01_06", "2020_03_13")+'.csv'
    
    df_covid.to_csv(covid_filename)
    
    covid.append(covid_filename)
    
    dst_path = pathdir+"//covid_period"
    
    if not os.path.exists(dst_path):
        os.makedirs(dst_path)
    
    for file in covid:
        shutil.move(file,dst_path+'\\'+file.split("\\")[1])
    
    os.remove(to_split)
    
if __name__ == '__main__':
    
   extension = 'csv'

   pathdir_cs = 'Schedule_Codeshare'
   pathdir_op = 'Schedule_Operating'
   
   
   eliminate_covid(pathdir_cs, extension)
   eliminate_covid(pathdir_op, extension)