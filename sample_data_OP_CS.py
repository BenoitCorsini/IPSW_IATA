import pandas as pd
import glob
import pickle
import os
# %% scripts to sample all file  

def sample_n_line_to_multiple_files(sample_dir, num_line,random_line=True):
    '''scripts to sample all file to multiple different files'''
        
    extension = 'csv'
    filenames = glob.glob(sample_dir+'/*.{}'.format(extension))

    if random_line:
        if not os.path.exists('sample_random/'+sample_dir):
            os.makedirs('sample_random/'+sample_dir+'/CSV/')
            os.makedirs('sample_random/'+sample_dir+'/Excel/')
            os.makedirs('sample_random/'+sample_dir+'/Pickel/')
        
        for i, file in enumerate(filenames): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            data_sample = pd.read_csv(file)
            data_sample.dropna(inplace=True)
            data_sample=data_sample.sample(num_line) #sample uniformly num_line
            
            data_sample.to_excel('sample_random/'+sample_dir+'/Excel/'+file[len(sample_dir)+1:len(file)-4:]+'.xlsx')
            data_sample.to_csv('sample_random/'+sample_dir+'/CSV/'+file[len(sample_dir)+1:len(file)-4:]+'.csv')
        
            with open('sample_random/'+sample_dir+'/Pickel/'+file[len(sample_dir)+1:len(file)-4:]+'.pkl','wb') as file:
                pickle.dump(data_sample,file)
    else:
        if not os.path.exists('sample_n_first_line/'+sample_dir):
            os.makedirs('sample_n_first_line/'+sample_dir+'/CSV/')
            os.makedirs('sample_n_first_line/'+sample_dir+'/Excel/')
            os.makedirs('sample_n_first_line/'+sample_dir+'/Pickel/')
            
        for i, file in enumerate(filenames): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            data_sample = pd.read_csv(file)
            data_sample.dropna(inplace=True)
            data_sample=data_sample.head(num_line)#sample num_line first line
            
            data_sample.to_excel('sample_n_first_line/'+sample_dir+'/Excel/'+file[len(sample_dir)+1:len(file)-4:]+'.xlsx')
            data_sample.to_csv('sample_n_first_line/'+sample_dir+'/CSV/'+file[len(sample_dir)+1:len(file)-4:]+'.csv')
        
            with open('sample_n_first_line/'+sample_dir+'/Pickel/'+file[len(sample_dir)+1:len(file)-4:]+'.pkl','wb') as file:
                pickle.dump(data_sample,file)



def sample_n_line(sample_dir, num_line,random_line=True):
    
    if random_line:
        if not os.path.exists('sample_random/'+sample_dir):
            os.makedirs('sample_random/'+sample_dir)
           
        
        extension = 'csv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
        data_sample = pd.read_csv(filenames[0])# Initialize dataframe
        data_sample.dropna(inplace=True)
        data_sample = data_sample.sample(num_line) #sample uniformly num_line
        
        for i, file in enumerate(filenames[1:]): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            x = pd.read_csv(file)
            x.dropna(inplace=True)
            x=x.sample(num_line) #sample uniformly num_line
            data_sample = data_sample.append(x)
            
      
        with open('sample_random/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
            pickle.dump(data_sample,file)
    
      
        
        data_sample.to_excel('sample_random/'+sample_dir+'/'+sample_dir+'.xlsx')
        data_sample.to_csv('sample_random/'+sample_dir+'/'+sample_dir+'.csv')
    
    else:
        if not os.path.exists('sample_n_first_line/'+sample_dir):
            os.makedirs('sample_n_first_line/'+sample_dir)
            
            
    
        extension = 'csv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
        data_sample = pd.read_csv(filenames[0]) # Initialize dataframe
        data_sample.dropna(inplace=True)
        data_sample = data_sample.head(num_line)
        
        for i, file in enumerate(filenames[1:]): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            x = pd.read_csv(file)
            x.dropna(inplace=True)
            x=x.head(num_line)
            data_sample = data_sample.append(x)
            
      
        with open('sample_n_first_line/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
            pickle.dump(data_sample,file)
    
   
        data_sample.to_excel('sample_n_first_line/'+sample_dir+'/'+sample_dir+'.xlsx')
        data_sample.to_csv('sample_n_first_line/'+sample_dir+'/'+sample_dir+'.csv')

def add_iosa_column(data_sample,iosa_info_data):
    
    with open(iosa_info_data,'rb') as file:
        process_IOSA = pickle.load(file)
            
    data_sample['Time series'] = pd.to_datetime(data_sample['Time series'],format="%Y-%m-%d %H:%M:%S")
 
    data_sample = data_sample[data_sample['IATA AL'].isin(process_IOSA['IATA Code'])]
   
    column_is_iosa=[]
    for index, row in data_sample.iterrows(): 
        current_process = process_IOSA[process_IOSA['IATA Code']==row['IATA AL']]
        is_iosa=0 
        for period in current_process['RegistrationPeriod'].values[0]:

            if row['Time series'] >= period[0] and row['Time series'] <= period[1]: #check if IOSA
                is_iosa=1
                break
        column_is_iosa.append(is_iosa)

    data_sample['Is IOSA']  = column_is_iosa 
    
    return data_sample

def sample_drop_num_stop_iata_column(sample_dir,iosa_info_data, num_line,random_line=True):
    
    if random_line:
        if not os.path.exists('sample_random_2/'+sample_dir):
            os.makedirs('sample_random_2/'+sample_dir)
           
        
        extension = 'csv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
        data_sample = pd.read_csv(filenames[0])# Initialize dataframe
        data_sample.dropna(inplace=True)
        data_sample =  data_sample[data_sample['No of Stops']==0]
        data_sample = data_sample.sample(num_line)
        
        for i, file in enumerate(filenames[1:]): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            x = pd.read_csv(file)
            x.dropna(inplace=True)
            x =  x[x['No of Stops']==0] #drop none 0 Number of Stops
            x=x.sample(num_line)
            data_sample = data_sample.append(x)
            
        print('Adding Is_IOSA Column')
        data_sample = add_iosa_column(data_sample,iosa_info_data)
        
        with open('sample_random_2/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
            pickle.dump(data_sample,file)
    
        data_sample.to_excel('sample_random_2/'+sample_dir+'/'+sample_dir+'.xlsx')
        data_sample.to_csv('sample_random_2/'+sample_dir+'/'+sample_dir+'.csv')


    else:
        if not os.path.exists('sample_n_first_line_2/'+sample_dir):
            os.makedirs('sample_n_first_line_2/'+sample_dir)
         
    
        extension = 'csv'
        filenames = glob.glob(sample_dir+'/*.{}'.format(extension))
        data_sample = pd.read_csv(filenames[0]) # Initialize dataframe
        data_sample.dropna(inplace=True)
        data_sample =  data_sample[data_sample['No of Stops']==0]
        data_sample = data_sample.head(num_line)
        
        for i, file in enumerate(filenames[1:]): # concatenate all dataframes together 
            print('processed %i of %i files' %(i+1,len(filenames)))
            x = pd.read_csv(file)
            x.dropna(inplace=True)
            x =  x[x['No of Stops']==0]
            x=x.head(num_line)
            data_sample = data_sample.append(x)
        
        print('Adding Is_IOSA Column')
        data_sample = add_iosa_column(data_sample,iosa_info_data)
        
        
        with open('sample_n_first_line_2/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
            pickle.dump(data_sample,file)
    
        data_sample.to_excel('sample_n_first_line_2/'+sample_dir+'/'+sample_dir+'.xlsx')
        data_sample.to_csv('sample_n_first_line_2/'+sample_dir+'/'+sample_dir+'.csv')

def date_range(year):
    return (str(year)+'-1-1',str(year)+'-12-31')

def sample_by_year(data_file,sample_dir,iosa_info_data,num_line,random_line=True):
    
    if random_line:
        if not os.path.exists('sample_random_year/'+sample_dir):
            os.makedirs('sample_random_year/'+sample_dir)
    
        with open(data_file,'rb') as file:
            data = pickle.load(file)
    
        data['Time series'] = pd.to_datetime(data['Time series'],format="%Y-%m-%d %H:%M:%S")
     
        
        for year in range(2011,2022):
            #study_start = pd.Timestamp(date_range(year)[0])
            #study_end = pd.Timestamp(date_range(year)[1])
            study_start =pd.to_datetime(date_range(year)[0],format="%Y-%m-%d %H:%M:%S")
            study_end =pd.to_datetime(date_range(year)[1],format="%Y-%m-%d %H:%M:%S")
            
            data_sample =data[data['Time series']>=study_start]
            data_sample= data_sample[data_sample['Time series']<=study_end]
            data_sample =  data_sample[data_sample['No of Stops']==0]
            data_sample = data_sample.sample(num_line)
            
            print('Adding Is_IOSA Column for year : ',year)
            
            data_sample = add_iosa_column(data_sample,iosa_info_data)
            
            with open('sample_random_year/'+sample_dir+'/'+sample_dir+'_'+str(year)+'.pkl','wb') as file:
                pickle.dump(data_sample,file)
        
            data_sample.to_excel('sample_random_year/'+sample_dir+'/'+sample_dir+'_'+str(year)+'.xlsx')
            data_sample.to_csv('sample_random_year/'+sample_dir+'/'+sample_dir+'_'+str(year)+'.csv')
    else:
        
        if not os.path.exists('sample_n_first_line_year/'+sample_dir):
            os.makedirs('sample_n_first_line_year/'+sample_dir)
    
        with open(data_file,'rb') as file:
            data = pickle.load(file)
    
        data['Time series'] = pd.to_datetime(data['Time series'],format="%Y-%m-%d %H:%M:%S")
     
        
        for year in range(2011,2022):
            #study_start = pd.Timestamp(date_range(year)[0])
            #study_end = pd.Timestamp(date_range(year)[1])
            study_start =pd.to_datetime(date_range(year)[0],format="%Y-%m-%d %H:%M:%S")
            study_end =pd.to_datetime(date_range(year)[1],format="%Y-%m-%d %H:%M:%S")
            
            data_sample =data[data['Time series']>=study_start]
            data_sample= data_sample[data_sample['Time series']<=study_end]
            data_sample =  data_sample[data_sample['No of Stops']==0]
            data_sample = data_sample.head(num_line)
            print('Adding Is_IOSA Column for year : ',year)
            data_sample = add_iosa_column(data_sample,iosa_info_data)
            
            with open('sample_n_first_line_year/'+sample_dir+'/'+sample_dir+'_'+str(year)+'.pkl','wb') as file:
                pickle.dump(data_sample,file)
        
            data_sample.to_excel('sample_n_first_line_year/'+sample_dir+'/'+sample_dir+'_'+str(year)+'.xlsx')
            data_sample.to_csv('sample_n_first_line_year/'+sample_dir+'/'+sample_dir+'_'+str(year)+'.csv')


def sample_by_year_airline(data_file,sample_dir,iosa_info_data,num_line,random_line=True):
    
    if random_line:
       
        with open(data_file,'rb') as file:
            data = pickle.load(file)
    
        data['Time series'] = pd.to_datetime(data['Time series'],format="%Y-%m-%d %H:%M:%S")
     
        
        for year in range(2011,2022):
            for airline in ['AC','EY']:
                if not os.path.exists('sample_randon_year_airline/'+airline+'/'+sample_dir):
                    os.makedirs('sample_randon_year_airline/'+airline+'/'+sample_dir)
    
                #study_start = pd.Timestamp(date_range(year)[0])
                #study_end = pd.Timestamp(date_range(year)[1])
                study_start =pd.to_datetime(date_range(year)[0],format="%Y-%m-%d %H:%M:%S")
                study_end =pd.to_datetime(date_range(year)[1],format="%Y-%m-%d %H:%M:%S")
                
                data_sample =data[data['Time series']>=study_start]
                data_sample= data_sample[data_sample['Time series']<=study_end]
                data_sample= data_sample[data_sample['IATA AL']==airline]
                data_sample =  data_sample[data_sample['No of Stops']==0]
                data_sample = data_sample.sample(num_line)
                
                print('Adding Is_IOSA Column for year : ',year)
                
                data_sample = add_iosa_column(data_sample,iosa_info_data)
                
                with open('sample_randon_year_airline/'+airline+'/'+sample_dir+'/'+sample_dir+'_'+airline+'_'+str(year)+'.pkl','wb') as file:
                    pickle.dump(data_sample,file)
            
                data_sample.to_excel('sample_randon_year_airline/'+airline+'/'+sample_dir+'/'+sample_dir+'_'+airline+'_'+str(year)+'.xlsx')
                data_sample.to_csv('sample_randon_year_airline/'+airline+'/'+sample_dir+'/'+sample_dir+'_'+airline+'_'+str(year)+'.csv')
    else:
        
        
        with open(data_file,'rb') as file:
            data = pickle.load(file)
    
        data['Time series'] = pd.to_datetime(data['Time series'],format="%Y-%m-%d %H:%M:%S")
     
        
        for year in range(2011,2022):
            for airline in ['AC','EY']:
                if not os.path.exists('sample_n_first_line_year_airline/'+airline+'/'+sample_dir):
                    os.makedirs('sample_n_first_line_year_airline/'+airline+'/'+sample_dir)
    
                #study_start = pd.Timestamp(date_range(year)[0])
                #study_end = pd.Timestamp(date_range(year)[1])
                study_start =pd.to_datetime(date_range(year)[0],format="%Y-%m-%d %H:%M:%S")
                study_end =pd.to_datetime(date_range(year)[1],format="%Y-%m-%d %H:%M:%S")
                
                data_sample =data[data['Time series']>=study_start]
                data_sample= data_sample[data_sample['Time series']<=study_end]
                data_sample= data_sample[data_sample['IATA AL']==airline]
                data_sample =  data_sample[data_sample['No of Stops']==0]
                data_sample = data_sample.head(num_line)
                print('Adding Is_IOSA Column for year : ',year)
                data_sample = add_iosa_column(data_sample,iosa_info_data)
                
                with open('sample_n_first_line_year_airline/'+airline+'/'+sample_dir+'/'+sample_dir+'_'+airline+'_'+str(year)+'.pkl','wb') as file:
                    pickle.dump(data_sample,file)
            
                data_sample.to_excel('sample_n_first_line_year_airline/'+airline+'/'+sample_dir+'/'+sample_dir+'_'+airline+'_'+str(year)+'.xlsx')
                data_sample.to_csv('sample_n_first_line_year_airline/'+airline+'/'+sample_dir+'/'+sample_dir+'_'+airline+'_'+str(year)+'.csv')


def sample_domestic_international(data_file,sample_dir,iosa_info_data,num_line,random_line=True):
    if random_line:
        with open(data_file,'rb') as file:
            data = pickle.load(file)
    
        data['Time series'] = pd.to_datetime(data['Time series'],format="%Y-%m-%d %H:%M:%S")
     
        
        for typ in ['Domestic','International']:
            if not os.path.exists('sample_random_domestic_international/'+typ+'/'+sample_dir):
                os.makedirs('sample_random_domestic_international/'+typ+'/'+sample_dir)
           
            data_sample =data[data['International/Domestic']==typ]
            data_sample =  data_sample[data_sample['No of Stops']==0]
            
            data_sample = data_sample.sample(num_line)
            
            print('Adding Is_IOSA Column...')
            data_sample = add_iosa_column(data_sample,iosa_info_data)
            
            with open('sample_random_domestic_international/'+typ+'/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
                pickle.dump(data_sample,file)
        
            data_sample.to_excel('sample_random_domestic_international/'+typ+'/'+sample_dir+'/'+sample_dir+'.xlsx')
            data_sample.to_csv('sample_random_domestic_international/'+typ+'/'+sample_dir+'/'+sample_dir+'.csv')


    else:
        with open(data_file,'rb') as file:
            data = pickle.load(file)
    
        data['Time series'] = pd.to_datetime(data['Time series'],format="%Y-%m-%d %H:%M:%S")
     
        
        for typ in ['Domestic','International']:
            if not os.path.exists('sample_n_first_line_domestic_international/'+typ+'/'+sample_dir):
                os.makedirs('sample_n_first_line_domestic_international/'+typ+'/'+sample_dir)
           
            data_sample =data[data['International/Domestic']==typ]
            data_sample =  data_sample[data_sample['No of Stops']==0]
            data_sample = data_sample.head(num_line)
            
            print('Adding Is_IOSA Column...')
            data_sample = add_iosa_column(data_sample,iosa_info_data)
            
            with open('sample_n_first_line_domestic_international/'+typ+'/'+sample_dir+'/'+sample_dir+'.pkl','wb') as file:
                pickle.dump(data_sample,file)
        
            data_sample.to_excel('sample_n_first_line_domestic_international/'+typ+'/'+sample_dir+'/'+sample_dir+'.xlsx')
            data_sample.to_csv('sample_n_first_line_domestic_international/'+typ+'/'+sample_dir+'/'+sample_dir+'.csv')


if __name__ == '__main__':  
    
    num_line = 500
    iosa_file_info = 'IOSA_info.pkl'
    
    #Generating Schedule CodeSchare sample
    
    CS_file ='schedule_CS.pkl'
    sample_dir_CS = 'Schedule_Codeshare'
    #sample_n_line(sample_dir_CS,num_line,random_line=True)
    #sample_n_line(sample_dir_CS,num_line,random_line=False)
    #sample_drop_num_stop_iata_column(sample_dir_CS,iosa_file_info,num_line,random_line=True)
    #sample_drop_num_stop_iata_column(sample_dir_CS,iosa_file_info,num_line,random_line=False)
    #sample_by_year(CS_file,sample_dir_CS,iosa_file_info,num_line,random_line=True)
    #sample_by_year(CS_file,sample_dir_CS,iosa_file_info,num_line,random_line=False)

    #sample_by_year_airline(CS_file,sample_dir_CS,iosa_file_info,num_line,random_line=True)
    #sample_by_year_airline(CS_file,sample_dir_CS,iosa_file_info,num_line,random_line=False)
    sample_domestic_international(CS_file,sample_dir_CS,iosa_file_info,num_line,random_line=True)
    sample_domestic_international(CS_file,sample_dir_CS,iosa_file_info,num_line,random_line=False)
    
    
    #sample_n_line_to_multiple_files(sample_dir_CS,num_line,random_line=True)
    
    #Generating Schedule Operating sample
    sample_dir_OP = 'Schedule_Operating'
    OP_file ='schedule_OP.pkl'
    #sample_n_line(sample_dir_OP,num_line,random_line=True)
    #sample_n_line(sample_dir_OP,num_line,random_line=False)
    #sample_drop_num_stop_iata_column(sample_dir_OP,iosa_file_info,num_line,random_line=True)
    #sample_drop_num_stop_iata_column(sample_dir_OP,iosa_file_info,num_line,random_line=False)
    #sample_by_year(OP_file,sample_dir_OP,iosa_file_info,num_line,random_line=True)
    #sample_by_year(OP_file,sample_dir_OP,iosa_file_info,num_line,random_line=False)
    #sample_n_line_to_multiple_files(sample_dir_OP,num_line,random_line=True)
    #sample_by_year_airline(OP_file,sample_dir_OP,iosa_file_info,num_line,random_line=True)
    #sample_by_year_airline(OP_file,sample_dir_OP,iosa_file_info,num_line,random_line=False)
    
    sample_domestic_international(OP_file,sample_dir_OP,iosa_file_info,num_line,random_line=True)
    sample_domestic_international(OP_file,sample_dir_OP,iosa_file_info,num_line,random_line=False)
    
    