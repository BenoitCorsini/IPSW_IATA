import os
import os.path as osp
import pandas as pd


AIR_DIR = 'Dataset/Airport & Airline List'
IOSA_DIR = 'Dataset/IOSA Registry'
CODESHARE_DIR = 'Dataset/Schedule_Codeshare'
OPERATING_DIR = 'Dataset/Schedule_Operating'
PASSENGER_DIR = 'Dataset/Passenger'



def load_airline(folder=AIR_DIR):
	'''
	81455 rows x 12 columns:
		'SK_AIRLINE',
		'NM_AIRLINE',
		'CD_IATA', ---CODE IATA---
		'CD_ICAO', ---CODE ICAO---
		'NK_ISOALPHA2COUNTRY',
		'CD_ISOALPHA3COUNTRY',
		'NM_REGIONIATA',
		'NM_REGIONSFO',
		'DT_VALIDFROM',
		'DT_VALIDTO',
		'IN_MEMBERIATA',
		'IN_MEMBERIOSA'
	'''
	df = pd.read_csv(osp.join(folder, 'DIMAIRLINE.csv'), delimiter=',')
	
	return df

def load_airport(folder=AIR_DIR):
	'''
	106466 rows x 14 columns:
		'SK_LOCATION',
		'NK_LOCATION',
		'NM_LOCATIONTYPE',
		'CD_LOCATIONIATA',
		'CD_LOCATIONICAO',
		'NM_LOCATION',
		'NO_LATITUDE',
		'NO_LONGITUDE',
		'NK_ISOALPHA2COUNTRY',
		'CD_ISOALPHA3COUNTRY',
		'NM_REGIONIATA',
		'CD_REGIONSFO',
		'DT_VALIDFROM',
		'DT_VALIDTO'
	'''
	df = pd.read_csv(osp.join(folder, 'DIMLOCATION.csv'), delimiter=',')

	return df

def load_IOSA(folder=IOSA_DIR):
	'''
	3464 rows x 9 columns:
		'Published',
		'Airline',
		'ICAO', ---CODE ICAO---
		'IATA Code', ---CODE IATA---
		'RegistrationExpiry',
		'AuditID',
		'Date of Audit Closure',
		'Region',
		'Country'
	'''
	df = pd.read_excel(osp.join(folder, 'IPSW - IOSA Registry.xlsx'))
	
	return df

def load_codeshares(folder=CODESHARE_DIR):
	'''
	Columns:
		'IATA AL', --Airline code--
		'Flight No',
		'Codeshare Car All', --List of codeshares--
		'Dep Airport Code',
		'Arr Airport Code',
		'International/Domestic',
		'No of Stops',
		'Stopping Airport',
		'Specific Aircraft Code',
		'Frequency',
		'Seats (Total)',
		'ASMs',
		'Time series'
	'''
	files = os.listdir(folder)[:1]

	df = []
	for file in files:
		assert file.endswith('.csv')
		df.append(pd.read_csv(osp.join(folder, file), delimiter=','))
	df = pd.concat(df, ignore_index=True)
	
	return df

def load_operating(folder=OPERATING_DIR):
	'''
	Columns:
		'IATA AL', --Airline code--
		'Flight No',
		'Codeshare Car All', --List of codeshares--
		'Dep Airport Code',
		'Arr Airport Code',
		'International/Domestic',
		'No of Stops',
		'Stopping Airport',
		'Frequency',
		'Seats (Total)',
		'ASMs',
		'Time series'
	'''
	files = os.listdir(folder)[:1]

	df = []
	for file in files:
		assert file.endswith('.csv')
		df.append(pd.read_csv(osp.join(folder, file), delimiter=','))
	df = pd.concat(df, ignore_index=True)
	
	return df

def load_passenger(folder=PASSENGER_DIR):
	'''
	Columns:
		'Travel Month',
		'Mkt Al 1', --Marketing Airlines--
		'Mkt Al 2',
		'Mkt Al 3',
		'Mkt Al 4',
		'Mkt Al 5',
		'Mkt Al 6',
		'Op Al 1', --Operating Airlines--
		'Op Al 2',
		'Op Al 3',
		'Op Al 4',
		'Op Al 5',
		'Op Al 6',
		'Orig Code', --IATA Code--
		'Dest Code', --IATA Code--
		'Seg Mkt Airline Code', 
		'Seg Op Al Code',
		'Seg Orig Airport Code',
		'Seg Dest Airport Code',
		'Reported + Est. Pax', --Number of passengers--
		'RPM', --Passenger x Miles--
	'''
	files = os.listdir(folder)[:1]

	df = []
	for file in files:
		_df = pd.read_csv(
			osp.join(folder, file),
			delimiter='\t',
			header=0,
			skiprows=5,
		)
		_df = _df.loc[:_df.shape[0] - 11,:]
		df.append(_df)
	df = pd.concat(df, ignore_index=True)

	return df
		

if __name__ == '__main__':
	print('NICE TRY')