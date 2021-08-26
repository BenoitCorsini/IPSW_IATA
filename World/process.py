import os
import os.path as osp
import json
import pandas as pd
import numpy as np


from animate import WorldAnimation
from config import PARAMS

'''
METRIC = 'ASMs'

### GETTING THE FLIGHTS ###
folder = '../Dataset/Plot'

files = os.listdir(folder)
df = []
name = ''
for file in files:
	df.append(pd.read_csv(osp.join(folder, file), index_col=0))
	name += f'__{file}__'
df = pd.concat(df)

flights = {}

for a1, a2, metric in zip(df['Dep Airport Code'], df['Arr Airport Code'], df[METRIC]):
	key = a1,a2
	if key not in flights:
		flights[key] = {'size' : 0}
	flights[key]['size'] += metric

def f(x):
	return x**.5
	return np.log(1 + x)

max_size = f(max(info['size'] for info in flights.values()))
print(max_size)
print(f(min(info['size'] for info in flights.values())))
print()
for flight_info in flights.values():
	flight_info['r'] = f(flight_info['size'])/max_size

print(len(flights))

### GETTING THE AIRPORTS ###
airports = {}

file = '../Dataset/Airport & Airline List/DIMLOCATION.csv'
df = pd.read_csv(file)

for a1, a2 in flights:
	if a1 not in airports:
		aux = df[df['CD_LOCATIONIATA'] == a1]
		airports[a1] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy()}
	if a2 not in airports:
		aux = df[df['CD_LOCATIONIATA'] == a2]
		airports[a2] = {'coord' : aux[['NO_LONGITUDE', 'NO_LATITUDE']].mean().to_numpy()}

print(len(airports))

### PLOTTING THE RESULTS ###
WA = WorldAnimation(airports=airports, flights=flights, params=PARAMS)
WA.make(name=name.replace('.csv', ''), n_angles = 90, n_rotations=2)
'''
WA = WorldAnimation(airports={}, flights={}, params=PARAMS)
WA.combine()
