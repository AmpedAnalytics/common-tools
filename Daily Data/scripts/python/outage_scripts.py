# required libraries
import os
import pandas as pd
import pymssql

datap = os.path.abspath(os.path.join(__file__,"../../../../data/"))
scriptp = os.path.abspath(os.path.join(__file__,"../../../scripts/"))


def get_coal_availability(duids, start_date):
    '''Collects MTPASA availabilty for all coal units for coming 7 days.
    Accepts coal DUIDS and a start date (today) as arguments.'''
    
    try:
        # establish connection
        with pymssql.connect(
            server="databases-plus-gr.public.f68d1dee025f.database.windows.net",
            port="3342",
            user="dept-energy",
            password="dirty-prejudge-MUTINEER-lyle-castrate",
            database="Historical-MMS"
        ) as connection:
            
            print("Querying database to collect MTPASA projections ...", end="\r")    
            # create cursor
            cursor = connection.cursor()
            
            # prepare arguments
            duid_list = "', '".join(duids)
            # end_date = start_date + pd.DateOffset(days=6)
            end_date = start_date + pd.DateOffset(days=14)

  
            # load query
            with open(os.path.join(scriptp,"sql/mtpasa_availability.sql"), "r") as file:
            #with open(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'sql','mtpasa_availability.sql')), "r") as file: #
                sql_query = file.read()

            # add arguments to query
            sql_query = sql_query.format(
                duid_list=duid_list,
                start_date=start_date,
                end_date=end_date
            ) 
            
            # execute query
            cursor.execute(sql_query)

            # return results in dataframe
            data = cursor.fetchall() # Data only, without column names
            columns = [row[0] for row in cursor.description] # .description gives column names
            mtpasa = pd.DataFrame(
                data=data,
                columns=columns
            )
            print("Querying database to collect MTPASA projections ... complete.")
            
            return mtpasa

    except pymssql.Error as e:
        print(f"Database error occurred: {e}")
        return None


def diagnose_outage(duid, outage_data,verbose=False):
    '''Applies logic to the DUID's diagnostic data to classify the outage as 'Planned',
    'Unplanned' or 'Unclear'.'''

    # set threshold
    outage_threshold = 5
    # check unit state
    unit_state = outage_data.at[duid, "os_pasaunitstate"]
    if ("UNPLAN" in unit_state) | ("FORCE" in unit_state):
        if verbose:
            print(f"{duid}: Unit state is {unit_state} at time of outage. Outage is unplanned.")
        return "Unplanned"
    if unit_state == "INACTIVERESERVE":
        if verbose:
            print(f"{duid}: Unit state is INACTIVERESERVE at time of outage. Outage is planned.")
        return "Planned"
    # check rampdown rate for a trip
    actual_mw = float(outage_data.at[duid, "actual_mw"])
    prior_mw = float(outage_data.at[duid, "mw_5min_prev"])
    rampdown_rate = (prior_mw - actual_mw) / 5
    max_rampdown_rate = outage_data.at[duid, "max_ramp_rate"]
    if rampdown_rate > max_rampdown_rate:
        if verbose:
            print(f"{duid}: Unit ramped down at unsafe speed. Outage is likely unplanned.")
        return "Unplanned"
    # check pasa_availability, which indicates planned outages
    pasaavail = float(outage_data.at[duid, "os_pasaavail"])
    if pasaavail < outage_threshold:
        if verbose:
            print(f"{duid}: PASA availability is (near) zero at time of outage. Outage is likely planned.")
        return "Planned"
    # check target MW versus actual MW
    target_mw = outage_data.at[duid, "target_mw"]
    if pd.notna(target_mw):
        target_mw = float(target_mw)
        output_diff = float(target_mw - actual_mw)
        if output_diff > max_rampdown_rate:
            if verbose:
                print(f"{duid}: Power is {output_diff:,.0f}MW below target at time of outage. Outage is likely unplanned.")
            return "Unplanned"
    # verify whether diagnostic data exists
    bidding_data = pd.notna(outage_data.at[duid, "bidofferdate"])
    if bidding_data:
        # check reason/explanation field
        reason = outage_data.at[duid, "rebidexplanation"].lower()
        if ("trip" in reason) | ("leak" in reason) | ("fault" in reason) | ("fail" in reason) | ("unexpect" in reason):
            if verbose:
                print(f"{duid}: Reason field suggests a failure. Outage is likely unplanned.")
            return "Unplanned"
        unit_state_now = outage_data.at[duid, "latest_pasaunitstate"]
        if pd.notna(unit_state_now):
            if ("UNPLAN" in unit_state_now) | ("FORCE" in unit_state_now):
                if verbose:
                    print(f"{duid}: The original unit state was {unit_state} but it is now {unit_state_now}. Outage is unplanned.")
                return "Unplanned"
            if ("PLAN" in unit_state_now) & ("UNPLAN" not in unit_state_now):
                if verbose:    
                    print(f"{duid}: The original unit state was {unit_state} but it is now {unit_state_now}. Outage is likely planned.")
                return "Planned"
    # final check if target MW indicates planned shutdown
    if pd.notna(target_mw):
        if target_mw < outage_threshold * 3:
            if verbose:
                print(f"{duid}: Target output is very low ({target_mw:,.0f}MW) at time of outage. Outage is likely planned.")
            return "Planned"
    # return 'unclear' if outage remains undefined
    if verbose:
        print(f"{duid}: The outage was unable to be classified automatically. PASA availability is above zero, ramp-down was safe and other checks were inconclusive. Outage type is unclear.")
    
    return "Unclear"


def get_outage_data(duids, day):
    '''Gathers the expected outage data and the expected return date of DUIDs using 
    updated MTPASA checks. Returns expected return date and other information .'''
    
    try:
        print("Collecting outage information and return dates for coal units ...", end="\r")
        
        # establish connection
        with pymssql.connect(
            server="databases-plus-gr.public.f68d1dee025f.database.windows.net",
            port="3342",
            user="dept-energy",
            password="dirty-prejudge-MUTINEER-lyle-castrate",
            database="Historical-MMS"
        ) as connection:        

            # create cursor
            cursor = connection.cursor()
            
            # prepare arguments
            duid_list = "', '".join(duids)
                                
            # load query
            with open(os.path.join(scriptp,"sql/coal_outages.sql"), "r") as file:
            #with open(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'sql','coal_outages.sql')), "r") as file:
                sql_query = file.read()

            # add arguments to query
            sql_query = sql_query.format(
                duid_list=duid_list,
                day=day
            )
            
            # execute query
            cursor.execute(sql_query)

            # return results in dataframe
            data = cursor.fetchall() # Data only, without column names
            columns = [row[0] for row in cursor.description] # .description gives column names
            outage_data = pd.DataFrame(
                data=data,
                columns=columns
            )
            # convert to dates
            date_cols = ["outage_start", "outage_end", "bidofferdate", "bidsettlementdate", "expected_return", "latest_offer_datetime"]
            for col in date_cols:
                outage_data[col] = pd.to_datetime(outage_data[col])
            
            # filter table to include only latest outage for each duid                        
            idx = outage_data.groupby("duid")["outage_start"].idxmax()
            outage_data = outage_data.loc[idx]

            print("Collecting outage information and return dates for coal units ... complete")
            
            return outage_data.set_index("duid")

    except pymssql.Error as e:
        print(f"Database error occurred: {e}")
        return None


def get_geninfo(duids):
    '''Load the latest gen info (coal unit names and capacities).'''
    
    # check for latest gen info file
    file_path = os.path.join(datap,"geninfo/")
    #file_path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '../..', 'data','geninfo')) ##
    geninfo_files = [file for file in os.listdir(file_path) if "geninfo" in file]
    geninfo_files.sort()
    latest_geninfo = geninfo_files[-1]
    
    # load file
    geninfo = pd.read_excel(
        os.path.join(file_path, latest_geninfo),
        sheet_name=4,
        skiprows=1,
        skipfooter=6,
        usecols=["Site Name", "DUID", "Upper Nameplate Capacity (MW)"],
        index_col="DUID"
    )
    geninfo = geninfo.loc[duids]
    geninfo.columns = ["name", "capacity"]
    # clean unit names
    geninfo["name"] = geninfo.index.map(lambda duid: f"{geninfo.at[duid, 'name'].replace(' Power Station', '').replace(' Power Plant', '')} {duid[-1]}")
    
    return geninfo


def get_historic_outages(today, lookback=31):
    '''
    Collect past month of outage data
    Determine on T-14 days 
    '''
    try:
        # establish connection
        with pymssql.connect(
            server="databases-plus-gr.public.f68d1dee025f.database.windows.net",
            port="3342",
            user="dept-energy",
            password="dirty-prejudge-MUTINEER-lyle-castrate",
            database="Historical-MMS"
        ) as connection:
            
            print(f"Querying database to collect past {lookback} days of outages ...", end="\r")    
            # create cursor
            cursor = connection.cursor()
            
            # prepare arguments
            end_date = pd.to_datetime(today) - pd.DateOffset(days=1)
            start_date = end_date - pd.DateOffset(days=lookback)
                              
            # load query
            with open(os.path.join(scriptp,"sql/outage_master_with_unit_status.sql"), "r") as file:
            #with open(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'sql','mtpasa_availability.sql')), "r") as file: #
                sql_query = file.read()

            # add arguments to query
            sql_query = sql_query.format(
                start_date=start_date.date(),
                end_date=end_date.date()
            ) 
            
            # execute query
            cursor.execute(sql_query)

            # return results in dataframe
            data = cursor.fetchall() # Data only, without column names
            columns = [row[0] for row in cursor.description] # .description gives column names
            df = pd.DataFrame(
                data=data,
                columns=columns
            )
            print(f"Querying database to collect past {lookback} days of outages ... complete.")

            df.to_csv("./data/historic_outages.csv",index=False)

            # df = pd.read_csv("./data/historic_outages.csv")

            ## for diagnosis
            df['current_status'] = 'RETURNED'

            df['region'] = df['REGIONID'].str.replace("1","")

            unit_state_dict = {
                    'OUTAGEPLANBASIC' : 'Planned',
                    'OUTAGEUNPLANFORCED' : 'Unplanned',
                    'OUTAGEUNPLANMAINT' : 'Unplanned',
                    'DERATINGUNPLANFORCED' : 'Unplanned',
                    'MOTHBALLED' : 'Unclear',
                    'INACTIVERESERVE' : 'Unclear',
                    'RETIRED' : 'Retired',
                    'OUTAGEPLANEXTEND' : 'Planned',
                    'DERATINGUNPLANMAINT' : 'Unplanned',
                    'DERATINGPLANBASIC' : 'Planned',
                    'DERATINGPLANEXTEND' : 'Planned',
                    'NODERATINGS' : 'Planned'
                }

            # this is an estimate only (visual so it matters less)
            df['outage_type'] = df['PASAUNITSTATE'].map(unit_state_dict)

            df = df[['DUID','region','OutageFrom','OutageTo','outage_type']]

            df.columns = ['duid','region','outage_date','expected_return','outage_type']

            df['outage_date'] = pd.to_datetime(df['outage_date'])
            df['expected_return'] = pd.to_datetime(df['expected_return'])

            df = df[df['expected_return']>(pd.to_datetime(today)-pd.DateOffset(days=lookback))]
            df = df[df['expected_return']<pd.to_datetime(today)]
            
            return df.copy()

    except pymssql.Error as e:
        print(f"Database error occurred: {e}")
        return None


