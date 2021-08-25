import pandas as pd
import glob
import pickle
from dask import dataframe as dd
from dask.distributed import Client
import os


'''
script to load all Passenger data files. 
Files are extremely large reccommend using dask and reducing data
'''

def pd_read_tsv_files(filename):

    sample_file = 'passenger/2017_01_Dynamic_Table_Estimate_756889.tsv'
    text = open(sample_file).readlines()
    titles = text[5].split(sep='\t')
    titles[-1] = titles[-1].rstrip()

    dtypes = {}

    for title in titles[1:-2]:
        dtypes[title] = 'str'

    dtypes[titles[-2]] = 'float64'
    dtypes[titles[-1]] = 'float64'

    passenger_data = pd.read_csv(filename, delimiter='\t', 
        skiprows=6, parse_dates=[titles[0]],
        names=titles, dtype=dtypes)
    
    passenger_data = passenger_data.loc[:passenger_data.shape[0] - 11,:]
    return passenger_data

def dask_read_tsv_files(filename):

    sample_file = 'passenger/2017_01_Dynamic_Table_Estimate_756889.tsv'
    text = open(sample_file).readlines()
    titles = text[5].split(sep='\t')
    titles[-1] = titles[-1].rstrip()

    dtypes = {}

    for title in titles[1:-2]:
        dtypes[title] = 'str'

    dtypes[titles[-2]] = 'float64'
    dtypes[titles[-1]] = 'float64'

    passenger_data =  dd.read_csv(filename, delimiter='\t', 
        skiprows=6, skipfooter = 12,
        parse_dates=[titles[0]],
        names=titles,dtype=dtypes,
        blocksize=64000000)# = 64 Mb chunks
    
    return passenger_data

    
    
def sample_data_to_many(sample_dir,num_line=None,fraction=None,random_line=True):
    
    if random_line:
        if not os.path.exists('sample_random/'+sample_dir):
            os.makedirs('sample_random/'+sample_dir+'/CSV/')
            os.makedirs('sample_random/'+sample_dir+'/Excel/')
            os.makedirs('sample_random/'+sample_dir+'/Pickel/')
      
 
        extension = 'tsv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
    
       
        for i,file in enumerate(filenames): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            
            passenger_data = pd_read_tsv_files(file)
        
            sample_passenger = passenger_data.sample(frac=fraction)
        
            sample_passenger = sample_passenger.compute()
            
            
            sample_passenger.to_excel('sample_random/'+sample_dir+'/Excel/'+file[len(sample_dir)+1:len(file)-4:]+'.xlsx')
            sample_passenger.to_csv('sample_random/'+sample_dir+'/CSV/'+file[len(sample_dir)+1:len(file)-4:]+'.csv')
        
            with open('sample_random/'+sample_dir+'/Pickel/'+file[len(sample_dir)+1:len(file)-4:]+'.pkl','wb') as file:
                pickle.dump(sample_passenger,file)
    else:
        if not os.path.exists('sample_n_first_line/'+sample_dir):
            os.makedirs('sample_n_first_line/'+sample_dir+'/CSV/')
            os.makedirs('sample_n_first_line/'+sample_dir+'/Excel/')
            os.makedirs('sample_n_first_line/'+sample_dir+'/Pickel/')
      
 
        extension = 'tsv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
    
       
        for i,file in enumerate(filenames): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            
            passenger_data = dask_read_tsv_files(file)
        
            sample_passenger = passenger_data.head(num_line)
            
            sample_passenger.to_excel('sample_n_first_line/'+sample_dir+'/Excel/'+file[len(sample_dir)+1:len(file)-4:]+'.xlsx')
            sample_passenger.to_csv('sample_n_first_line/'+sample_dir+'/CSV/'+file[len(sample_dir)+1:len(file)-4:]+'.csv')
        
            with open('sample_n_first_line/'+sample_dir+'/Pickel/'+file[len(sample_dir)+1:len(file)-4:]+'.pkl','wb') as file:
                pickle.dump(sample_passenger,file)
                
        
def sample_data_to_one(sample_dir,num_line=None,fraction=None,random_line=True):
    
    if random_line:
        if not os.path.exists('sample_random/'+sample_dir):
            os.makedirs('sample_random/'+sample_dir)
           
     
        extension = 'tsv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
        
        data_sample = pd_read_tsv_files(filenames[0]).sample(num_line)
        
        for i,file in enumerate(filenames[1:]): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            
            passenger_data = pd_read_tsv_files(file)
        
            sample_passenger = passenger_data.sample(num_line)
             
            data_sample = data_sample.append(sample_passenger)
         
        data_sample.to_excel('sample_random/'+sample_dir+'/'+sample_dir+'.xlsx')
        data_sample.to_csv('sample_random/'+sample_dir+'/'+sample_dir+'.csv')
    
        with open('sample_random/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
            pickle.dump(data_sample,file)
            
    else:
        if not os.path.exists('sample_n_first_line/'+sample_dir):
            os.makedirs('sample_n_first_line/'+sample_dir)
           
      
     
        extension = 'tsv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
     
        data_sample = dask_read_tsv_files(filenames[0]).head(num_line)
        for i,file in enumerate(filenames[1:]): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            
            passenger_data = dask_read_tsv_files(file)
           
            sample_passenger = passenger_data.head(num_line)
            data_sample = data_sample.append(sample_passenger)
            
        data_sample.to_excel('sample_n_first_line/'+sample_dir+'/'+sample_dir+'.xlsx')
        data_sample.to_csv('sample_n_first_line/'+sample_dir+'/'+sample_dir+'.csv')
    
        with open('sample_n_first_line/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
            pickle.dump(data_sample,file)
        
            
    
        
if __name__ == '__main__': 
    client = Client(n_workers=2) 
    num_line = 500
    sample_dir = 'Passenger'
    sample_data_to_one(sample_dir,num_line=num_line,random_line=True)
    #sample_data_to_one(sample_dir,num_line=num_linerandom_line=False)
    #sample_data_to_many(sample_dir,num_line=num_line,random_line=True)
    #sample_data_to_many(sample_dir,num_line,random_line=False) 
       
        
    
