import os
import os.path as osp
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd



colours = {
	'OP' : {
		'line' : 'black',
		'IOSA' : 'royalblue',
		'nonIOSA' : 'crimson',
	},
	'CS' : {
		'line' : 'black',
		'IOSA' : 'royalblue',
		'nonIOSA' : 'crimson',
	},
}
alpha=0.3



folder = '../Dataset/Stats'

df = []
for file in sorted(os.listdir(folder)):
	df_ = pd.read_csv(osp.join(folder, file), index_col=0)
	df_['Operated'] = 'Operating' in file
	df.append(df_)
df = pd.concat(df)
df = df[['IATA AL', 'ASMs', 'Time series', 'Operated', 'Is IOSA']]

ALs = df['IATA AL'].unique()
for AL in ALs:
	dfAL = df[df['IATA AL'] == AL]
	plotter = {}
	for line in dfAL.iterrows():
		line = line[1]
		key = line['Time series']
		year, month, _ = key.split('-')
		key = int(year)*12 + int(month)
		if key not in plotter:
			plotter[key] = {
				'is_IOSA' : line['Is IOSA'],
				'ASM_OP' : 0,
				'ASM_CS' : 0,
			}
		assert plotter[key]['is_IOSA'] == line['Is IOSA']
		if line['Operated']:
			plotter[key]['ASM_OP'] += line['ASMs']
		else:
			plotter[key]['ASM_CS'] += line['ASMs']

	X = sorted(list(plotter))
	YOP = [plotter[x]['ASM_OP'] for x in X]
	YCS = [plotter[x]['ASM_CS'] for x in X]
	where = [(not plotter[x]['is_IOSA']) for x in X]

	OP = [plotter[x]['ASM_OP']*(not plotter[x]['is_IOSA']) for x in X]
	CS = [plotter[x]['ASM_CS']*(not plotter[x]['is_IOSA']) for x in X]

	plt.figure(figsize=(16,9))
	plt.axis(xmin=min(X), xmax=max(X), ymin=0, ymax=1.1*max(YOP+YCS))
	for year in range(2011, 2022):
		plt.plot([year*12]*2, [-1, 1.5*max(YOP+YCS)], 'lightgray', zorder=-1)

	years = range(int(min(X)/12), int(max(X)/12)+1)
	plt.xticks([12*year for year in years], [str(year) for year in years])
	#plt.fill_between(X, YOP, color='white', lw=0, zorder=-0.5)
	plt.fill_between(X, YCS, color='white', lw=0, zorder=-0.5)
	#plt.fill_between(X, YOP, color=colours['OP']['IOSA'], lw=0, alpha=alpha, zorder=0)
	plt.fill_between(X, YCS, color=colours['CS']['IOSA'], lw=0, alpha=alpha, zorder=0, label='IOSA')
	#plt.fill_between(X, OP, where=where, color='white', lw=0, zorder=0.5)
	plt.fill_between(X, CS, where=where, color='white', lw=0, zorder=0.5)
	#plt.fill_between(X, OP, where=where, color=colours['OP']['nonIOSA'], lw=0, alpha=alpha, zorder=1)
	plt.fill_between(X, CS, where=where, color=colours['CS']['nonIOSA'], lw=0, alpha=alpha, zorder=1, label='non-IOSA')
	#plt.plot(X, YOP, colours['OP']['line'], zorder=2, label='Operated')
	plt.plot(X, YCS, colours['CS']['line'], zorder=2, label='ASM')
	plt.legend()
	plt.savefig(f'{AL}.png')