import pandas as pd
import pyodbc
import requests
import urllib
import os
import os.path as path
from bs4 import BeautifulSoup
import re
from pandas.testing import assert_frame_equal
from pandas._libs.tslibs.parsing import DateParseError

datap =  path.abspath(path.join(__file__,"../../../../data/"))
scriptp =  path.abspath(path.join(__file__,"../../../scripts/"))


# change to pymssql
def get_nem_price_and_demand(location,start_date):
    # DCCEEW log-in
    server = "databases-plus-gr.public.f68d1dee025f.database.windows.net"
    port = "3342"
    username = "dept-energy"
    password = "dirty-prejudge-MUTINEER-lyle-castrate"
    database = "Historical-MMS"

    # establish connection
    connection = pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}"
    )
    cursor = connection.cursor()

    # load SQL query
    with open(path.join(scriptp,"sql/get_price_and_demand.sql"), "r") as sql_file:
        sql_query = sql_file.read()
    
    # fetch data into a list of tuples
    cursor.execute(sql_query, (start_date))
    data = cursor.fetchall()

    # create dataFrame from the tuples
    df = pd.DataFrame(
        [tuple(row) for row in data],
        columns=[column[0] for column in cursor.description]    
    )

    # close connection
    connection.close()

    # clean and save dataframe
    df.DateStamp = pd.to_datetime(df.DateStamp) # convert to date
    df.sort_values("DateStamp", inplace=True)
    df.to_csv(location, index=False)

    return df.copy()


def get_nem_prices(location,archive=path.join(datap,"NEM_Prices.csv"),d=5,manual=False):
    # grab the last 24hrs to complete various calculations
    if manual:
        folder_path = path.join(datap,"fill/NEM/")
        files = [file for file in os.listdir(folder_path)]
        if len(files) != 1:
            print("Please ensure only the csv with the required dates is in the folder.")
            os.startfile(folder_path)
            return None
        else:
            recent = pd.read_csv(path.join(folder_path,files[0]),index_col=False)
            recent["DateStamp"] = pd.to_datetime(recent.DateStamp)
    else:
        yesterday = pd.Timestamp.today() - pd.Timedelta(days=d)
        recent = get_nem_price_and_demand(location,yesterday.strftime("%Y-%m-%d"))

    # Prepare the dataframe for merging by making all numericals into floats and resetting index
    recent.reset_index(drop=True,inplace=True)
    recent[["Price","Demand"]] = recent[["Price","Demand"]].astype(float)

    # Prepare for merge by keeping relevant columns and setting DateStamp to datetime object
    historical = pd.read_csv(archive,index_col=False)
    historical = historical[["DateStamp","Region","Price","Demand"]]
    historical["DateStamp"] = pd.to_datetime(historical.DateStamp)

    # Complete the merge
    df = pd.merge(historical, recent, on=["DateStamp", "Region","Price","Demand"], how="outer")
    df.sort_values("DateStamp",inplace=True)
    df.drop_duplicates(inplace=True)
    
    # Save to the original historical data file
    df.to_csv(archive,index=False)

    return df.copy()

def get_wem_prices(archive=path.join(datap,"WEM_Prices.csv"),manual=False):
    if manual:
        folder_path = path.join(datap,"fill/WEM/")
        files = [file for file in os.listdir(folder_path)]
        if len(files) != 1:
            print("Please ensure only the csv with the required dates is in the folder.")
            os.startfile(folder_path)
            return None
        else:
            recent = pd.read_csv(path.join(folder_path,files[0]),index_col=False)
    else:
        recent = pd.read_csv("https://app.nemreview.info/api/trends/12758b72-5297-40dc-a8f5-fb5b71c2cb04/files/csv")
    historical = pd.read_csv(archive,index_col=False)

    # clean up the api-called csv - wa prices for last 5 days
    recent.columns = ["DateStamp","Price","Demand"]
    recent["DateStamp"] = pd.to_datetime(recent.DateStamp, format="%d-%b-%Y %H:%M")
    historical["DateStamp"] = pd.to_datetime(historical.DateStamp)

    # Complete the merge
    df = pd.merge(historical, recent, on=["DateStamp","Price","Demand"], how="outer")
    df.sort_values("DateStamp",inplace=True)
    df.drop_duplicates(subset=["DateStamp"],inplace=True)

    df.index = pd.to_datetime(df["DateStamp"])

    # Save to the original historical data file
    df.to_csv(archive,index=False)

    return df.copy()

def get_urls(url, ext=""):
    '''
    Gets the html from the webpage and finds all relevant file urls
    Param url: url of website as string
    Param ext: extention of file as string
    Return parent: returns list of urls with relevant ext
    '''
    response = requests.get(url)
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, 'html.parser')
   
    #grab file urls from html with "<a href>"
    parent = [node.get('href') for node in soup.find_all("a") if node.get('href') != None if node.get('href').startswith('https') if node.get('href').endswith(ext)]
    return parent

def extract_date(link):
    return f"20{link[-11:-9]}-{link[-9:-7]}-{link[-7:-5]}"

def merge_dkis(file, archive = path.join(datap,"DKIS_Prices.csv")):
    
    try:
        recent = pd.read_excel(file,skiprows=4,index_col=0)
    except DateParseError:
        recent = pd.read_excel(file,skiprows=4,usecols=[1,2,3])

    recent.index.name = 'DateStamp'     

    recent = recent[['Market Price','Demand']]
    recent.columns = ["Price","Demand"]

    time = recent.index.astype(str)
    today = extract_date(file)

    actual_time = []
    for hour in time:
        threshold = pd.to_datetime("04:30:00")
        if pd.to_datetime(hour) < threshold:
            temp = pd.to_datetime(today) + pd.Timedelta(days=1)
            actual_time.append(pd.to_datetime(temp.strftime("%Y-%m-%d ") + hour))
        else:
            actual_time.append(pd.to_datetime(today + " " + hour))

    recent["DateStamp"] = pd.to_datetime(actual_time)
    recent.reset_index(drop=True,inplace=True)

    historical = pd.read_csv(archive)
    historical["DateStamp"] = pd.to_datetime(historical.DateStamp)
    historical[["Price","Demand"]] = historical[["Price","Demand"]].astype(float)

    # Complete the merge
    df = pd.merge(historical, recent, on=["DateStamp","Price","Demand"], how="outer")
    df.sort_values(by="DateStamp",inplace=True)
    df.drop_duplicates(subset=["DateStamp"],inplace=True)

    # Save to the original historical data file
    df.to_csv(archive,index=False)

    return df.copy()

def get_dkis_prices(days=5,manual=False):
    '''
    takes list of urls and writes new files in a folder with content
    Param date: date as string format 'YYYY-MM-DD'
    return: True as Bool and date of latest file as string format 'YYYY-MM-DD'
    '''
    if manual:
        for file in os.listdir(path.join(datap,"fill/DKIS")):
            df = merge_dkis(path.join(datap,"fill/DKIS/"+file)) 
    else:
        # url = 'https://www.powerwater.com.au/market-operator/daily-price-and-trading-data'
        url = 'https://ntesmo.com.au/data/daily-trading'
        ext = '.xlsx'
        result = get_urls(url, ext)
        if days < 1:
            print("days must be at least 1.")
            return None
        else:
            for file in result:
                # print(file)
                if days < 1:
                    break

                try:
                    df = merge_dkis(file)
                    days-=1

                except urllib.error.URLError as e:
                    print("Error occured: ", type(e), e)
                    # some weird SSL thing is happening
                    urllib.request.urlretrieve(file, path.join(datap, "test.xls"))
                    os.rename(path.join(datap, "test.xls"),path.join(datap, f"{file[-11:]}"))
                    file = path.join(datap, f"{file[-11:]}")

                    df = merge_dkis(file)
                    days-=1

    return df.copy()

def get_aemo_reserves():
    data = pd.read_csv("https://visualisations.aemo.com.au/aemo/data/NEM/SEVENDAYOUTLOOK/WEB_SEVENDAY_OUTLOOK.CSV")
    data = data[["CALENDAR_DATE","REGIONID","DATATYPE","DATAVALUE"]]

    df1 = data[data["DATATYPE"]=="Scheduled Demand"]
    df2 = data[data["DATATYPE"]=="Scheduled Reserve"]

    df1 = pd.pivot_table(df1,index="REGIONID",columns="CALENDAR_DATE",values="DATAVALUE",aggfunc="first")
    df2 = pd.pivot_table(df2,index="REGIONID",columns="CALENDAR_DATE",values="DATAVALUE",aggfunc="first")
    df1.columns = pd.to_datetime(df1.columns).strftime("%#d-%b-%y")
    df2.columns = pd.to_datetime(df2.columns).strftime("%#d-%b-%y")

    df1.to_csv(path.join(datap,"demand.csv"))
    df2.to_csv(path.join(datap,"reserves.csv"))

    df = pd.concat([df1,df2],ignore_index=False, axis=1)
    new_order = [0,1,2,4,3]  # New order of rows
    df = df.take(new_order)

    return df.copy()

def get_asx_futures():
    urls = ["https://www.asxenergy.com.au/futures_au/dataset/N",
            "https://www.asxenergy.com.au/futures_au/dataset/Q",
            "https://www.asxenergy.com.au/futures_au/dataset/S",
            "https://www.asxenergy.com.au/futures_au/dataset/V"]
    futures = {"N":[],"V":[],"Q":[],"S":[]}
    for url in urls:
        try:
            response = requests.get(url)
        except requests.exceptions.SSLError:
            return None
        if response.ok:
            response_text = response.text
        else:
            return response.raise_for_status()
        soup = BeautifulSoup(response_text, 'html.parser')
        # Find all tr elements
        tr_elements = soup.find_all('tr')

        this_year = pd.Timestamp.today().year

        # Extract the last td field if the instrument is CY25 or CY26
        last_values = []
        for tr in tr_elements:
            td_elements = tr.find_all('td')
            if len(td_elements) > 1:  # Ensure there are at least two td elements
                instrument = td_elements[0].text.strip()
                if instrument == f'CY{str(this_year+1)[-2:]}' or instrument == f'CY{str(this_year+2)[-2:]}':
                    last_value = td_elements[-1].text.strip()
                    last_values.append(last_value)
        futures[url[-1]] = last_values[:2]

    df = pd.DataFrame(futures)
    df.index = [f'CY{str(this_year+1)[-2:]}',f'CY{str(this_year+2)[-2:]}']
    df = df.transpose()

    if df.isnull().sum().any():
        print("Could not extract data - tables not populated.")
        return None
    
    new_order = [0,2,3,1]  # New order of rows
    df = df.take(new_order)
    df = df.astype(float)
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    df.index.name = today

    df.to_csv(path.join(datap,"futures.csv"),index_label=today)

    return df.copy()

def _get_baseload_futures():

    df = pd.read_csv("https://app.nemreview.info/api/trends/69551cc5-01cf-4ee9-b8ec-023fd28e78cb/files/csv")

    return df.copy()