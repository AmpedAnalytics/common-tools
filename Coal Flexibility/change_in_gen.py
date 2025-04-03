import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np


coal_duids = ['BW01', 'BW02', 'BW03', 'BW04', 'CALL_B_1',
    'CALL_B_2', 'CPP_3', 'CPP_4', 'ER01', 'ER02',
    'ER03', 'ER04', 'GSTONE1', 'GSTONE2', 'GSTONE3',
    'GSTONE4', 'GSTONE5', 'GSTONE6', 'KPP_1', 'LYA1',
    'LYA2', 'LYA3', 'LYA4', 'LOYYB1', 'LOYYB2',
    'MPP_1', 'MPP_2', 'MP1', 'MP2', 'STAN-1',
    'STAN-2', 'STAN-3', 'STAN-4', 'TARONG#1', 'TARONG#2',
    'TARONG#3', 'TARONG#4', 'TNPS1', 'VP5', 'VP6',
    'YWPS1', 'YWPS2', 'YWPS3', 'YWPS4']

for duid in coal_duids[2:]:

    print(duid)

    df = pd.read_csv(f"coal_flexibility/data/{duid}.csv",index_col=0)
    df.index.name = 'Date'
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)

    ramp = df.diff() / 5

    up = ramp[ramp>0]
    down = ramp[ramp<0]

    up.columns=['Ramp Rate MW/min (up)']
    down.columns=['Ramp Rate MW/min (up)']

    ax = up.plot(alpha=0.9)
    down.plot(ax=ax,alpha=0.9)
    ax = plt.plot()
    plt.savefig(f"coal_flexibility/graphs/{duid}_ramp.png",dpi=100)


    seasonal = df.copy()
    daily = df.copy()
    seasonal.columns = ['seasonal']
    daily.columns = ['daily']

    seasonal = seasonal.resample('ME').mean().pct_change()
    seasonal = seasonal.replace([np.inf, -np.inf], np.nan).dropna(axis=0)

    # print(max(seasonal['seasonal'][1:].to_list()))

    daily = daily.resample('1h').sum().pct_change()
    daily = daily[daily<max(seasonal['seasonal'])] # make this less than the maximum of seasonal
    ax = daily.plot(figsize=(10,6),grid=True)

    seasonal.plot(ax=ax)
    ax = plt.plot()
    plt.savefig(f"coal_flexibility/graphs/{duid}_gen.png",dpi=100)

    plt.close()
