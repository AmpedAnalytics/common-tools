import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
from matplotlib.legend_handler import HandlerPatch
import matplotlib.patches as mpatches

class HandlerSquare(HandlerPatch):
    def create_artists(self, legend, orig_handle,
                       xdescent, ydescent, width, height, fontsize, trans):
        center = xdescent + 0.5 * (width - height), ydescent
        p = mpatches.Rectangle(xy=center, width=height,
                               height=height, angle=0.0)
        self.update_prop(p, orig_handle, legend)
        p.set_transform(trans)
        return [p]      

def graph_aemo_reserves(demand=os.path.realpath(os.path.join(__file__,"../../../../data/demand.csv")),reserves=os.path.realpath(os.path.join(__file__,"../../../../data/reserves.csv"))):
    # Set initial parameters
    plt.rcParams["hatch.linewidth"] = 1
    plt.rcParams["text.color"] = "#808080"

    barWidth = 0.4

    df1 = pd.read_csv(demand)
    df2 = pd.read_csv(reserves)
    df1.drop(3,inplace=True)
    df2.drop(3,inplace=True)

    x = df1.columns[1:]

    r1 = np.arange(len(x))
    r2 = [x + barWidth for x in r1]
    r3 = [x + 0.2 for x in r2]

    # Plot
    statecolor = ["orange","tomato","green","blue"]
    state = ["NSW","QLD","SA","VIC"]

    fig, ax = plt.subplots(2,2,figsize=(14,7))

    counter=0
    for i in (0,1):
        for j in (0,1):
            d = df1.iloc[counter,1:].to_list()
            r = df2.iloc[counter,1:].to_list()
            print(demand,reserves)
            rects1 = ax[i,j].bar(r1, d, color="#808080", width=barWidth-0.05)
            rects2 = ax[i,j].bar(r2, r, width=barWidth-0.05, color="none", 
                        facecolor=statecolor[counter], edgecolor="white", hatch=r"\\\\\\")
            ax[i,j].bar(r2, r, width=barWidth-0.05, color="none", 
                        edgecolor="black", linestyle="dashed")
            ax[i,j].bar(r3, r, color="none",edgecolor="none")

            ax[i,j].spines['top'].set_visible(False)
            ax[i,j].spines['bottom'].set_color('gray')
            ax[i,j].spines['right'].set_visible(False)
            ax[i,j].spines['left'].set_visible(False)

            ax[i,j].tick_params(left = False) 
            ax[i,j].tick_params(axis="x",color = "gray",labelcolor="gray",labelsize=9) 
            ax[i,j].tick_params(axis="y",labelcolor="gray") 

            ax[i,j].set_xticks([w + barWidth/2 for w in range(len(r))], x)
            
            ax[i,j].set_axisbelow(True)
            ax[i,j].yaxis.grid(color='gray',linewidth=0.3)

            ax[i,j].set_title(f'{state[counter]} Outlook - Reserve & Capacity (MW)', fontname='Calibri', fontsize=16)
            ax[i,j].legend( (rects1[0], rects2[0]), (f"{state[counter]} Demand", f"{state[counter]} Reserves"), 
                            handler_map={rects1[0]: HandlerSquare(), rects2[0]: HandlerSquare()},
                            loc='lower center', bbox_to_anchor=(0.5, -0.25),
                            ncol=2, fancybox=True, shadow=True)

            counter+=1

    # Save plot
    plt.tight_layout()
    plt.savefig(os.path.realpath(os.path.join(__file__,"../../../OUTPUT/ELECTRICITY/Demand and Reserves.png")),dpi=100)