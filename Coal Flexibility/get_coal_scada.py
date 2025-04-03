import pymssql
import pandas as pd
import os

# DUIDs for queries and matching other information
coal_duids = ['BW01', 'BW02', 'BW03', 'BW04', 'CALL_B_1',
    'CALL_B_2', 'CPP_3', 'CPP_4', 'ER01', 'ER02',
    'ER03', 'ER04', 'GSTONE1', 'GSTONE2', 'GSTONE3',
    'GSTONE4', 'GSTONE5', 'GSTONE6', 'KPP_1', 'LYA1',
    'LYA2', 'LYA3', 'LYA4', 'LOYYB1', 'LOYYB2',
    'MPP_1', 'MPP_2', 'MP1', 'MP2', 'STAN-1',
    'STAN-2', 'STAN-3', 'STAN-4', 'TARONG#1', 'TARONG#2',
    'TARONG#3', 'TARONG#4', 'TNPS1', 'VP5', 'VP6',
    'YWPS1', 'YWPS2', 'YWPS3', 'YWPS4']

# paths

outer_path = os.path.realpath(os.path.join(__file__,"../../"))

# collect generation data 
try:
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
        
        for duid in coal_duids[4:]:
            print(duid)
            # load query
            with open(os.path.join(outer_path,"scripts/coal_gen_2000.sql"), "r") as file:
                sql_query = file.read()

            # add arguments to query
            sql_query = sql_query.format(
                duid=duid
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
            df.to_csv(os.path.join(outer_path,f'data/{duid}.csv'),index=False)
            print(f"Generation since 2000 for {duid} ... complete.\n")
        
except pymssql.Error as e:
    print(f"Database error occurred: {e}")