import os
import os.path as osp
import json
import pandas as pd
import numpy as np


from animate import WorldAnimation
from config import PARAMS

###########################################################################
######### COMPARING IOSA AND NON_IOSA WITH OPERTING AND CODESHARE #########
###########################################################################

if False:
	for is_IOSA in [0,1]:
		if is_IOSA:
			first_title = 'IOSA '
		else:
			first_title = 'non-IOSA '
		for OCS in ['Operating', 'Codeshare']:
			title = first_title + OCS.replace('ting', 'ted').replace('share', 'shared') + ' Flights'
			name = f'{is_IOSA}_{OCS}'
			print(f'IS IOSA: {is_IOSA} -- TYPE: {OCS.upper()}')
			print()

			### GETTING THE FLIGHTS ###
			folder = '../Dataset/Plot'

			files = os.listdir(folder)
			df = []
			for file in files:
				if OCS in file:
					df.append(pd.read_csv(osp.join(folder, file), index_col=0))
			df = pd.concat(df)
			df = df[df['Is IOSA'] == is_IOSA]

			flights = {}

			for a1, a2, freq, n_seats in zip(df['Dep Airport Code'], df['Arr Airport Code'], df['Frequency'], df['Seats (Total)']):
				key = a1,a2
				if key not in flights:
					flights[key] = {'size' : 0}
				flights[key]['size'] += freq*n_seats

			def f(x):
				return x**.5
				return np.log(1 + x)

			max_size = f(max(info['size'] for info in flights.values()))
			print(max_size)
			print(f(min(info['size'] for info in flights.values())))
			for flight_info in flights.values():
				flight_info['r'] = f(flight_info['size'])/max_size
			print()
			print(len(flights))
			print()

			### GETTING THE AIRPORTS ###
			airports = {}

			file = '../Dataset/Airport & Airline List/DIMLOCATION.csv'
			df = pd.read_csv(file)

			for a1, a2 in flights:
				if a1 not in airports:
					aux = df[df['CD_LOCATIONIATA'] == a1]
					airports[a1] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy(), 'size' : flights[a1, a2]['size']}
				else:
					airports[a1]['size'] = max(airports[a1]['size'], flights[a1, a2]['size'])
				if a2 not in airports:
					aux = df[df['CD_LOCATIONIATA'] == a2]
					airports[a2] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy(), 'size' : flights[a1, a2]['size']}
				else:
					airports[a2]['size'] = max(airports[a2]['size'], flights[a1, a2]['size'])

			def g(x):
				return x**.5
				return np.log(1 + x)

			max_size = g(max(info['size'] for info in airports.values()))
			print(max_size)
			print(g(min(info['size'] for info in airports.values())))
			for airport_info in airports.values():
				airport_info['r'] = g(airport_info['size'])/max_size
			print()
			print(len(airports))
			print()

			### PLOTTING THE RESULTS ###
			WA = WorldAnimation(airports=airports, flights=flights, params=PARAMS)
			WA.make(name=name, frames_dir=name, n_angles = 8, n_rotations=1, title=title, plot_airplanes=False)

	WA = WorldAnimation(airports={}, flights={}, params=PARAMS)
	for is_IOSA in [0, 1]:
		if is_IOSA:
			name = 'IOSA'
		else:
			name = 'non-IOSA'
		WA.combine(name=name, frames_dir=name, folder_1=f'{is_IOSA}_Operating', folder_2=f'{is_IOSA}_Codeshare')
	for OCS in ['Operating', 'Codeshare']:
		WA.combine(name=OCS, frames_dir=OCS, folder_1=f'1_{OCS}', folder_2=f'0_{OCS}')


#################################################################
######### COMPARING THE IMPACT OF CODESHARE ON AIRLINES #########
#################################################################

if False:
	file = '../Dataset/Airport & Airline List/DIMLOCATION.csv'
	AIRPORTS = list(set(pd.read_csv(file)['CD_LOCATIONIATA']))
	for CD in ['AC', 'EY']:
		if CD == 'AC':
			first_title = 'Air Canada '
		else:
			first_title = 'Etihad '
		for OCS in ['Operating', 'Codeshare']:
			title = first_title + OCS.replace('ting', 'ted').replace('share', 'shared') + ' Flights'
			name = f'{CD}_{OCS}'
			print(f'CODE: {CD} -- TYPE: {OCS.upper()}')
			print()

			### GETTING THE FLIGHTS ###
			folder = '../Dataset/Plot'

			files = sorted(os.listdir(folder))
			df = []
			for file in files:
				if (CD in file) & (OCS in file):
					df.append(pd.read_csv(osp.join(folder, file), index_col=0))
			df = pd.concat(df)

			flights = {}

			for a1, a2, freq, n_seats in zip(df['Dep Airport Code'], df['Arr Airport Code'], df['Frequency'], df['Seats (Total)']):
				if (a1 in AIRPORTS) & (a2 in AIRPORTS):
					if (a1, a2) not in flights:
						flights[a1, a2] = {'size' : 0}
					flights[a1, a2]['size'] += freq*n_seats

			def f(x):
				return x#**.5
				return np.log(1 + x)

			max_size = f(max(info['size'] for info in flights.values()))
			print(max_size)
			print(f(min(info['size'] for info in flights.values())))
			for flight_info in flights.values():
				flight_info['r'] = f(flight_info['size'])/max_size
			print()
			print(len(flights))
			print()

			### GETTING THE AIRPORTS ###
			airports = {}

			file = '../Dataset/Airport & Airline List/DIMLOCATION.csv'
			df = pd.read_csv(file)

			for a1, a2 in flights:
				if a1 not in airports:
					aux = df[df['CD_LOCATIONIATA'] == a1]
					if aux.shape[0] == 0:
						print(f'PROBLEM WITH AIRPORT: {a1}')
					airports[a1] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy(), 'size' : flights[a1, a2]['size']}
				else:
					airports[a1]['size'] = max(airports[a1]['size'], flights[a1, a2]['size'])
				if a2 not in airports:
					aux = df[df['CD_LOCATIONIATA'] == a2]
					if aux.shape[0] == 0:
						print(f'PROBLEM WITH AIRPORT: {a2}')
					airports[a2] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy(), 'size' : flights[a1, a2]['size']}
				else:
					airports[a2]['size'] = max(airports[a2]['size'], flights[a1, a2]['size'])

			def g(x):
				return x#**.5
				return np.log(1 + x)

			max_size = g(max(info['size'] for info in airports.values()))
			print(max_size)
			print(g(min(info['size'] for info in airports.values())))
			for airport_info in airports.values():
				airport_info['r'] = g(airport_info['size'])/max_size
			print()
			print(len(airports))
			print()

			### PLOTTING THE RESULTS ###
			WA = WorldAnimation(airports=airports, flights=flights, params=PARAMS)
			WA.make(name=name, frames_dir=name, n_angles=180, n_rotations=5, title=title)

if True:
	WA = WorldAnimation(airports={}, flights={}, params=PARAMS)
	for CD in ['EY']:#['AC', 'EY']:
		WA.combine(name=CD, frames_dir=CD, folder_1=f'{CD}_Operating', folder_2=f'{CD}_Codeshare')

#############################################################
######### COMPARING THE IMPACT OF COVID ON AIRLINES #########
#############################################################

if False:
	file = '../Dataset/Airport & Airline List/DIMLOCATION.csv'
	AIRPORTS = list(set(pd.read_csv(file)['CD_LOCATIONIATA']))
	for CD in ['AC']:#, '2020']:
		if CD == 'AC':
			first_title = 'Air Canada '
		else:
			first_title = 'Etihad '
		for YEAR in ['2019', '2020']:
			#title = first_title + OCS.replace('ting', 'ted').replace('share', 'shared') + ' Flights'
			title = f'Air Canada Operated Flights - {YEAR}'
			name = f'{CD}_{YEAR}'
			print(f'CODE: {CD} -- YEAR: {YEAR}')
			print()

			### GETTING THE FLIGHTS ###
			folder = '../Dataset/Plot'

			files = sorted(os.listdir(folder))
			df = []
			for file in files:
				if (CD in file) & (YEAR in file):
					df.append(pd.read_csv(osp.join(folder, file), index_col=0))
			df = pd.concat(df)

			flights = {}

			for a1, a2, freq, n_seats in zip(df['Dep Airport Code'], df['Arr Airport Code'], df['Frequency'], df['Seats (Total)']):
				if (a1 in AIRPORTS) & (a2 in AIRPORTS):
					if (a1, a2) not in flights:
						flights[a1, a2] = {'size' : 0}
					flights[a1, a2]['size'] += freq*n_seats

			def f(x):
				return x#**.5
				return np.log(1 + x)

			max_size = f(max(info['size'] for info in flights.values()))
			print(max_size)
			print(f(min(info['size'] for info in flights.values())))
			for flight_info in flights.values():
				flight_info['r'] = f(flight_info['size'])/max_size
			print()
			print(len(flights))
			print()

			### GETTING THE AIRPORTS ###
			airports = {}

			file = '../Dataset/Airport & Airline List/DIMLOCATION.csv'
			df = pd.read_csv(file)

			for a1, a2 in flights:
				if a1 not in airports:
					aux = df[df['CD_LOCATIONIATA'] == a1]
					if aux.shape[0] == 0:
						print(f'PROBLEM WITH AIRPORT: {a1}')
					airports[a1] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy(), 'size' : flights[a1, a2]['size']}
				else:
					airports[a1]['size'] = max(airports[a1]['size'], flights[a1, a2]['size'])
				if a2 not in airports:
					aux = df[df['CD_LOCATIONIATA'] == a2]
					if aux.shape[0] == 0:
						print(f'PROBLEM WITH AIRPORT: {a2}')
					airports[a2] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy(), 'size' : flights[a1, a2]['size']}
				else:
					airports[a2]['size'] = max(airports[a2]['size'], flights[a1, a2]['size'])

			def g(x):
				return x#**.5
				return np.log(1 + x)

			max_size = g(max(info['size'] for info in airports.values()))
			print(max_size)
			print(g(min(info['size'] for info in airports.values())))
			for airport_info in airports.values():
				airport_info['r'] = g(airport_info['size'])/max_size
			print()
			print(len(airports))
			print()

			### PLOTTING THE RESULTS ###
			WA = WorldAnimation(airports=airports, flights=flights, params=PARAMS)
			WA.make(name=name, frames_dir=name, n_angles=180, n_rotations=5, title=title)

if False:
	WA = WorldAnimation(airports={}, flights={}, params=PARAMS)
	for CD in ['AC']:#['AC', 'EY']:
		WA.combine(name=CD, frames_dir=CD, folder_1=f'{CD}_2019', folder_2=f'{CD}_2020')