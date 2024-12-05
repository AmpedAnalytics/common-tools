from get_data import get_nem_prices, get_wem_prices, get_dkis_prices, \
                     get_asx_futures, get_aemo_reserves
from grapher import graph_aemo_reserves
from price_calculator import wholesale_electricy_price
from coal_outages import get_coal_outages
import os
import shutil
from pandas import ExcelWriter,Timestamp,to_datetime,DataFrame,Timestamp,date_range
import pandas as pd
import time
import argparse
import textwrap

parent = os.path.realpath(os.path.join(__file__,"../../../"))
data_path = os.path.realpath(os.path.join(__file__,"../../../data/"))
output_path = os.path.realpath(os.path.join(__file__,"../../../OUTPUT/ELECTRICITY/"))

def workbook_history(arx_path = os.path.join(parent,"archive/")):
    try:
        for file_name in os.listdir(output_path):
            # print(file_name)
            if file_name.endswith("TODAY.xlsx"):
                source = os.path.join(output_path,file_name)
                destination = os.path.join(arx_path, file_name[:-11] + ".xlsx")
            
                if os.path.isfile(source):
                    shutil.copy(source, destination)
                    
                    today = Timestamp.today().strftime("%Y-%m-%d")
                    os.rename(source,os.path.join(output_path,f"Daily Data {today} TODAY.xlsx"))

                    print('Archived: ', file_name,'\n')
            elif file_name.startswith("MDD"):
                today = Timestamp.today().strftime("%#d %b %Y")
                new_file_name = f"MDD Electricity Data Workbook - {today}.xlsx"
                os.rename(os.path.join(output_path,file_name),os.path.join(output_path,new_file_name))

    except FileNotFoundError:
        print("WARNING: The 'data' file does not exist. \
               Program will not run without dummy file called TODAY.xlsx")

    # DELETE DATES MORE THAN A MONTH OLD
    arx_list = os.listdir(arx_path)
    if len(arx_list) > 31:
        print("Deleting older files...")
        sorted_list = []
        temp_list = arx_list.copy()
        while len(arx_list) != len(sorted_list):
            temp = temp_list.pop()
            date = to_datetime(temp[-15:-5])
            sorted_list.append((date,temp))
        
        sorted_list.sort(key=lambda a: a[0])

        for fn in sorted_list[:len(sorted_list)-7]:
            print(sorted_list)
            os.remove(arx_path + fn[1])

def get_daily_data(manual=False):
    ### NEM
    print("Getting NEM prices...")
    try:
        nem = get_nem_prices(os.path.join(data_path,"NEM_recent.csv"),manual=manual)
        if not isinstance(nem,DataFrame):
            print("Couldn't get NEM prices.\n")
        else:
            print("Got NEM prices!\n")
    except Exception as e:
        print("Error occured: ", type(e), e)
        print("Please manually get NEM prices!!!\n")

    ### WEM
    print("Getting WEM prices...")
    try:
        wem = get_wem_prices(manual=manual)
        if not isinstance(wem,DataFrame):
            print("Couldn't get WEM prices.\n")
        else:
            print("Got WEM prices!\n")
    except Exception as e:
        print("Error occured: ", type(e), e)
        print("Please manually get WEM prices!!!\n")

    ### DKIS
    print("Getting DKIS prices...")
    try:
        get_dkis_prices(manual=manual)
        print("Got DKIS prices!\n")
    except Exception as e:
        print("Error occured: ", type(e), e)
        print("Please manually get DKIS prices!!!\n")

    if not manual:
        ### ASX
        print("Getting Futures prices...")
        try:
            futures=get_asx_futures()

            if isinstance(futures,DataFrame):
                print("Got Futures prices!")
            else:
                print("WARNING ASX futures table not populated or could not connect \
                        - please try running again after 8:30 am!\n \
                        https://www.asxenergy.com.au/futures_au")
        except Exception as e:
            print("Error occured: ", type(e), e)
            print("Please manually get Futures prices!!!")
            
        ### AEMO
        print("Getting 7 day outlook...")
        try:
            outlook = get_aemo_reserves()
            print("Got 7 day outlook!")
        except Exception as e:
            print("Error occured: ", type(e), e)
            print("Please manually get 7 Day Outlook!!!")
    else:
        outlook = None

    return outlook


### DETECT IF PREVIOUS PRICE BEING LOWER / HIGHER CHANGED


def write_daily_data(outlook,
                     nemdata  = (os.path.join(data_path,"NEM_Prices.csv")),
                     wadata   = (os.path.join(data_path, "WEM_Prices.csv")),
                     dkisdata = (os.path.join(data_path,"DKIS_Prices.csv")),
                     futuresdata  = (os.path.join(data_path,"futures.csv"))):

    nem_prices = wholesale_electricy_price(nemdata,nem=True,wem=False)
    wem_prices = wholesale_electricy_price(wadata,nem=False,wem=True)
    dkis_prices = wholesale_electricy_price(dkisdata,nem=False,wem=False)

    # check for any gaps in data - eventually fill in gaps automatically
    nem_gap = check_discontinuity(nem_prices.price.index)
    wem_gap = check_discontinuity(wem_prices.price.index,five_min_settlement=False)
    dkis_gap = check_discontinuity(dkis_prices.price.index,five_min_settlement=False)

    print("NEM Missing Dates: ",nem_gap)
    print("WEM Missing Dates: ",wem_gap)
    print("DKIS Missing Dates: ",dkis_gap)

    nem_summary = nem_prices.summarise()
    wem_summary = wem_prices.summarise()
    dkis_summary = dkis_prices.summarise()
    futures_summary = pd.read_csv(futuresdata)

    with ExcelWriter(os.path.join(output_path,f'Daily Data {nem_prices.today} TODAY.xlsx'), engine='openpyxl', mode='a', if_sheet_exists='replace') as w:
        nem_summary.to_excel(w, sheet_name='NEM INFO', index=True)
        wem_summary.to_excel(w, sheet_name='WEM INFO', index=False)
        dkis_summary.to_excel(w, sheet_name='DKIS INFO', index=False)
        futures_summary.to_excel(w, sheet_name='ASX', index=False, index_label=futures_summary.index.name)
        outlook.to_excel(w, sheet_name='7 Day Outlook', index=True)

def check_discontinuity(dates,five_min_settlement=True):
    if five_min_settlement:
        discon = date_range(
            start=dates[0],end=dates[-1],freq='5min').difference(dates)
    else:
        discon = date_range(
            start=dates[0],end=dates[-1],freq='30min').difference(dates)
    return list(discon)

def ddee(dd,outages,futures,write,update,manual):
    start=time.time()

    if not update:
        workbook_history()

    if dd==False and outages==False and futures==False and not manual:
        dd=True
        outages=True

    if dd:
        if not write:
            outlook = get_daily_data(manual)
        else:
            df1 = pd.read_csv(os.path.join(data_path,"demand.csv"))
            df2 = pd.read_csv(os.path.join(data_path,"reserves.csv"))

            df = pd.concat([df1,df2],ignore_index=False, axis=1)
            new_order = [0,1,2,4,3]  # New order of rows
            outlook = df.take(new_order)

        write_daily_data(outlook)

        graph_aemo_reserves()

        print("Daily Data and Reserves Done!\n")
        print("Getting Coal Outages next...\n")

    if outages and not write:
        get_coal_outages()
        print("Got Coal Outages!\n")

    if futures:
        if not write:
            get_asx_futures()

        future_data = pd.read_csv(os.path.join(data_path,"futures.csv"))
        today = Timestamp.today().strftime("%Y-%m-%d")

        with ExcelWriter(os.path.join(output_path,f'Daily Data {today} TODAY.xlsx'), engine='openpyxl', mode='a', if_sheet_exists='replace') as w:
            future_data.to_excel(w, sheet_name='ASX', index=False, index_label=future_data.index.name)

        print("Futures done!\n")
        
    if manual:
        get_daily_data(manual)

        print("Manual data upload done!")
    
    return start

if __name__=="__main__":
    par = argparse.ArgumentParser(prog='Daily Data and Energy Essentials',
                                  formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent("""\
                                  !-------------------------------------------------------------
                                  A new method of doing daily data and energy essentials that is 
                                  
                                  t o o o o o o o o o o o o t a l l y EPIC and N E V E R breaks!
                                  -------------------------------------------------------------!

                                  You can add cool args like:

                                        --write:         skips the get_data phase (e.g. if you've already done it manually)
                                        --dd:            runs all daily data (and energy essentials) except coal outages
                                        --futures:       runs futures only
                                        --dd --outages:  runs dd and outages (same as not having them)

                                    par exemple, some common use cases:

                                        python main.py --update --futures
                                        python main.py --update --outages
                                                              
                                                    OR

                                        python main.py --manual --update
                                        python main.py --write --dd                 
                                        
                                    the order of the arguments won't matter."""),
                                  epilog=textwrap.dedent("""\
                                  Seriously if it breaks, I will buy everyone 1 million coffees...
                                                                                                    ...hopefully no-one is actually reading this part...\n"""))

    par.add_argument("--write", action="store_true",
                     help="enable writing only (skips get_data)")
    par.add_argument("--update", action="store_true",
                     help="disable archive function and updates current workbook")
    par.add_argument("--manual", action="store_true",
                     help="""1. only downloads data in './data/fill/' (skips writing to './OUTPUT/')\n
                             2. prompts drag and dropping of files to fill in missing data if automatic update fails (run --write after this)""")
    
    # can pick and choose which to do, if none chosen does all
    par.add_argument("--dd", action="store_true",
                     help="enable Daily Data")
    par.add_argument("--futures", action="store_true",
                     help="enable Futures")
    par.add_argument("--outages", action="store_true",
                     help="enable Coal Outages")
    
    args = par.parse_args()
    
    task = (args.dd,args.outages,args.futures,args.write,args.update,args.manual)

    all_args_false = all(not t for t in task)

    if all_args_false:
        print(textwrap.dedent("""\n\n\
              ---------------------------------------------------------------------------
              Daily data 4.2 has more options. Try running main.py -h to see all options!
              ---------------------------------------------------------------------------\n\n"""))
        
    start=ddee(*task)
    end=time.time()
    print(f"\nThat took {round(end-start,2)} seconds!")

    os.startfile(os.path.realpath(output_path))