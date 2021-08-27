import pickle
from dask import dataframe as dd
from dask.distributed import Client
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate

from query_schedule import convert_IOSA, convert_CS_OP, save_data, check_folder
    
def calculate_proportions(group, i=0):

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

    airline_IOSA_CS = len(group['IATA AL'].loc[mask_IOSA_CS].unique())
    airline_IOSA_OP = len(group['IATA AL'].loc[mask_IOSA_OP].unique())
    airline_nonIOSA_CS = len(group['IATA AL'].loc[mask_nonIOSA_CS].unique())
    airline_nonIOSA_OP = len(group['IATA AL'].loc[mask_nonIOSA_OP].unique())

    total_groups_airline = airline_IOSA_CS + airline_IOSA_OP + airline_nonIOSA_CS + airline_nonIOSA_OP
    total_airlines = len(group['IATA AL'].unique())

    row = {}

    row['year'] = group['Time series'].iloc[0].year
    row['IOSA Codeshared'               ] =     n_IOSA_CS
    row['IOSA Operated'                 ] =     n_IOSA_OP
    row['nonIOSA Codeshared'            ] =     n_nonIOSA_CS
    row['nonIOSA Operated'              ] =     n_nonIOSA_OP
    row['IOSA Codeshared ASMs'          ] =     asm_IOSA_CS
    row['IOSA Operated ASMs'            ] =     asm_IOSA_OP
    row['nonIOSA Codeshared ASMs'       ] =     asm_nonIOSA_CS
    row['nonIOSA Operated ASMs'         ] =     asm_nonIOSA_OP
    row['IOSA Codesharing Airlines'     ] =     (airline_IOSA_CS / total_groups_airline) * total_airlines
    row['IOSA Operating Airlines'       ] =     (airline_IOSA_OP / total_groups_airline) * total_airlines
    row['nonIOSA Codesharing Airlines'  ] =     (airline_nonIOSA_CS / total_groups_airline) * total_airlines
    row['nonIOSA Operating Airlines'    ] =     (airline_nonIOSA_OP / total_groups_airline) * total_airlines

    x = pd.Series(data=row, name=i)

    return x

if __name__ == '__main__': 

    # schedule_data = pd.read_csv("Schedule_sample.csv", index_col=0) # Initialize dataframe

    # schedule_data['Is IOSA'] = schedule_data.apply(convert_IOSA, axis=1)
    # schedule_data['CS_OP'] = schedule_data.apply(convert_CS_OP, axis=1)
    # schedule_data.rename(columns={'CS_OP':'Is CS'}, inplace=True)
    # schedule_data['Time series'] = pd.to_datetime(schedule_data['Time series'],format="%Y-%m-%d %H:%M:%S")

    with open('Schedule.pkl','rb') as file:
        schedule_data = pickle.load(file)

    # airline_list = ['A5','VJ','AC','EY']
    airline = 'EY'
    filtered_data = schedule_data[schedule_data['IATA AL'].isin([airline,])]

    airline = ''
    filtered_data = schedule_data

    # Groupby airport pair and year
    group_data = filtered_data.groupby( [pd.Grouper(key='Time series', freq=('1Y')),], as_index=False)

    columnsAgg = ['year',
                  'IOSA Codeshared',
                  'IOSA Operated',
                  'nonIOSA Codeshared',
                  'nonIOSA Operated',
                  'IOSA Codeshared ASMs',
                  'IOSA Operated ASMs',
                  'nonIOSA Codeshared ASMs',
                  'nonIOSA Operated ASMs']

    x = calculate_proportions(filtered_data)

    total = x['IOSA Codesharing Airlines'] + x['IOSA Operating Airlines'] + x['nonIOSA Codesharing Airlines'] + x['nonIOSA Operating Airlines']

    p_IOSA_CS = x['IOSA Codesharing Airlines'] / total
    p_IOSA_OP = x['IOSA Operating Airlines'] / total
    p_nonIOSA_CS = x['nonIOSA Codesharing Airlines'] / total
    p_nonIOSA_OP = x['nonIOSA Operating Airlines'] / total

    data = [['CS'       , p_IOSA_CS             , p_nonIOSA_CS,                 ],
            ['OP'       , p_IOSA_OP             , p_nonIOSA_OP,                 ],
            ['Total'    ,p_IOSA_CS + p_IOSA_OP  , p_nonIOSA_CS + p_nonIOSA_OP,  ]] 
        
    print(tabulate(data, headers=['', 'IOSA', 'nonIOSA']))

    filtered_data_agg = pd.DataFrame(columns=columnsAgg)

    keys = []; i = 0
    for key, group in group_data:

        x = calculate_proportions(group, i)
        filtered_data_agg = filtered_data_agg.append(x)

        keys += [key]
        i += 1

    filtered_data_agg = filtered_data_agg.set_index('year')

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
    filtered_data_agg[columns].plot(kind='bar', ax=ax, stacked=True, color=color)
    ax.set_ylabel("Number of scheduled flights",fontsize=14)
    ax.set_xlabel("Year",fontsize=14)
    plt.legend(loc='center left', bbox_to_anchor=(0.1, 1.1), ncol=2)

    fig.savefig('images/nFlights_%s.png' %(airline), format='png', dpi=300, bbox_inches='tight')
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
    filtered_data_agg[columns].plot(kind='bar', ax=ax, stacked=True, color=color)
    ax.set_ylabel("Available Seat Miles (ASMs)",fontsize=14)
    ax.set_xlabel("Year",fontsize=14)
    plt.legend(loc='center left', bbox_to_anchor=(0.0, 1.15), ncol=2)

    fig.savefig('images/ASMs_%s.png' %(airline), format='png', dpi=300, bbox_inches='tight')
    plt.show()

    # Airlines plot
    columns = ['IOSA Codesharing Airlines',
               'IOSA Operating Airlines',
               'nonIOSA Codesharing Airlines',
               'nonIOSA Operating Airlines'  ]

    color={"IOSA Codesharing Airlines": "#ff4d4d", 
        "IOSA Operating Airlines": "#ffcccc",
        "nonIOSA Codesharing Airlines": "#0066ff",
        "nonIOSA Operating Airlines": "#cce0ff",}

    fig = plt.figure()
    ax = fig.gca()
    filtered_data_agg[columns].plot(kind='bar', ax=ax, stacked=True, color=color)
    ax.set_ylabel("Number of airlines",fontsize=14)
    ax.set_xlabel("Year",fontsize=14)
    plt.legend(loc='center left', bbox_to_anchor=(0.0, 1.15), ncol=2)

    fig.savefig('images/Airlines_%s.png' %(airline), format='png', dpi=300, bbox_inches='tight')
    plt.show()