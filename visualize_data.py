import pickle
from dask import dataframe as dd
from dask.distributed import Client
import pandas as pd
import matplotlib.pyplot as plt

from query_schedule import convert_IOSA, convert_CS_OP, save_data, check_folder

    
if __name__ == '__main__': 

    # schedule_data = pd.read_csv("Schedule_sample.csv", index_col=0) # Initialize dataframe

    # schedule_data['Is IOSA'] = schedule_data.apply(convert_IOSA, axis=1)
    # schedule_data['CS_OP'] = schedule_data.apply(convert_CS_OP, axis=1)
    # schedule_data.rename(columns={'CS_OP':'Is CS'}, inplace=True)
    # schedule_data['Time series'] = pd.to_datetime(schedule_data['Time series'],format="%Y-%m-%d %H:%M:%S")

    with open('Schedule.pkl','rb') as file:
        schedule_data = pickle.load(file)

    # airline_list = ['A5','VJ','AC','EY']
    schedule_data = schedule_data[schedule_data['IATA AL'].isin(['A5',])]

    # Groupby airport pair and year
    group_data = schedule_data.groupby( [pd.Grouper(key='Time series', freq=('1Y')),], as_index=False)

    columnsAgg = ['year',
                  'IOSA Codeshared',
                  'IOSA Operated',
                  'nonIOSA Codeshared',
                  'nonIOSA Operated',
                  'IOSA Codeshared ASMs',
                  'IOSA Operated ASMs',
                  'nonIOSA Codeshared ASMs',
                  'nonIOSA Operated ASMs']

    schedule_data_agg = pd.DataFrame(columns=columnsAgg)

    keys = []; i = 0
    for key, group in group_data:

        mask_IOSA_CS = (group['Is IOSA'] == 1) & (group['Is CS'] == 1)
        mask_IOSA_OP = (group['Is IOSA'] == 1) & (group['Is CS'] == 0)
        mask_nonIOSA_CS = (group['Is IOSA'] == 0) & (group['Is CS'] == 1)
        mask_nonIOSA_OP = (group['Is IOSA'] == 0) & (group['Is CS'] == 0)

        n_IOSA_CS = len(group.loc[mask_IOSA_CS])
        n_IOSA_OP = len(group.loc[mask_IOSA_OP])
        n_nonIOSA_CS = len(group.loc[mask_nonIOSA_CS])
        n_nonIOSA_OP = len(group.loc[mask_nonIOSA_OP])

        asm_IOSA_CS = group['ASMs'].loc[mask_IOSA_CS].sum()
        asm_IOSA_OP = group['ASMs'].loc[mask_IOSA_OP].sum()
        asm_nonIOSA_CS = group['ASMs'].loc[mask_nonIOSA_CS].sum()
        asm_nonIOSA_OP = group['ASMs'].loc[mask_nonIOSA_OP].sum()

        row = {}

        row['year'] = group['Time series'].iloc[0].year
        row['IOSA Codeshared'           ] =     n_IOSA_CS
        row['IOSA Operated'             ] =     n_IOSA_OP
        row['nonIOSA Codeshared'        ] =     n_nonIOSA_CS
        row['nonIOSA Operated'          ] =     n_nonIOSA_OP
        row['IOSA Codeshared ASMs'      ] =     asm_IOSA_CS
        row['IOSA Operated ASMs'        ] =     asm_IOSA_OP
        row['nonIOSA Codeshared ASMs'   ] =     asm_nonIOSA_CS
        row['nonIOSA Operated ASMs'     ] =     asm_nonIOSA_OP

        x = pd.Series(data=row, name=i)
        schedule_data_agg = schedule_data_agg.append(x)

        keys += [key]
        i += 1

    schedule_data_agg = schedule_data_agg.set_index('year')

    check_folder('images')

    # Number of flights plot
    columns = ['IOSA Codeshared',
                'IOSA Operated',
                'nonIOSA Codeshared',
                'nonIOSA Operated']

    color={"IOSA Codeshared": "#ff4d4d", 
        "IOSA Operated": "#ffcccc",
        "nonIOSA Codeshared": "#0066ff",
        "nonIOSA Operated": "#cce0ff",}

    fig = plt.figure()
    ax = fig.gca()
    schedule_data_agg[columns].plot(kind='bar', ax=ax, stacked=True, color=color)
    ax.set_ylabel("Number of scheduled flights",fontsize=14)
    ax.set_xlabel("Year",fontsize=14)
    plt.legend(loc='center left', bbox_to_anchor=(0.1, 1.1), ncol=2)

    fig.savefig('images/nFlights.png', format='png', dpi=300, bbox_inches='tight')
    plt.show()

    # ASMs plot
    columns = ['IOSA Codeshared ASMs',
                'IOSA Operated ASMs',
                'nonIOSA Codeshared ASMs',
                'nonIOSA Operated ASMs']

    color={"IOSA Codeshared ASMs": "#ff4d4d", 
        "IOSA Operated ASMs": "#ffcccc",
        "nonIOSA Codeshared ASMs": "#0066ff",
        "nonIOSA Operated ASMs": "#cce0ff",}

    fig = plt.figure()
    ax = fig.gca()
    schedule_data_agg[columns].plot(kind='bar', ax=ax, stacked=True, color=color)
    ax.set_ylabel("Available Seat Miles (ASMs)",fontsize=14)
    ax.set_xlabel("Year",fontsize=14)
    plt.legend(loc='center left', bbox_to_anchor=(0.0, 1.15), ncol=2)

    fig.savefig('images/ASMs.png', format='png', dpi=300, bbox_inches='tight')
    plt.show()