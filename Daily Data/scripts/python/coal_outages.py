# import dependencies
from copy import copy
from openpyxl import load_workbook
from openpyxl.styles.borders import Border, Side
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# import local scripts
from outage_scripts import (
    get_coal_availability,
    diagnose_outage,
    get_outage_data,
    get_geninfo,
    get_historic_outages
)

test_date = pd.to_datetime("2025-01-15").date()

def get_coal_outages():

    output_path = os.path.realpath(os.path.join(__file__,"../../../OUTPUT/"))
    data_path = os.path.realpath(os.path.join(__file__,"../../../../data/"))


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

    # load latest geninfo (coal unit names and capacities)
    geninfo = get_geninfo(coal_duids)

    # run SQL functions
    today = pd.to_datetime("today").date()

    # ! test a different day !
    # today = test_date

    # mtpasa = get_coal_availability(
    #     coal_duids,
    #     today + pd.DateOffset(days=1)
    # )

    # mtpasa['DAY'] = pd.to_datetime(mtpasa['DAY'])

    # outage_data = get_outage_data(
    #     coal_duids,
    #     today
    # )

    # historic_outages = get_historic_outages(
    #     today,
    #     lookback=15 # can change duration looking backwards
    # )

    # mtpasa.to_csv(os.path.join(data_path,"mtpasa_example.csv"),index=False)
    # outage_data.to_csv(os.path.join(data_path,"outage_example.csv"))
    # historic_outages.to_csv(os.path.join(data_path,"historic_outage_example.csv"),index=False)

    # ### DEBGUG START
    
    # Comment above and uncomment below for debugging

    mtpasa = pd.read_csv(os.path.join(data_path,"mtpasa_example.csv"),parse_dates=['DAY'])
    outage_data = pd.read_csv(os.path.join(data_path,"outage_example.csv"),index_col=0,parse_dates=[
                  'outage_start','outage_end','bidofferdate','bidsettlementdate','expected_return','latest_offer_datetime'])
    historic_outages = pd.read_csv(os.path.join(data_path,"historic_outage_example.csv"),parse_dates=[
                  'outage_date','expected_return'])
    
    ### DEBUG END

    def process_info(outage_data):
        # identify units that are out on day of reporting
        # include any DUID listed with 'M'
        current_duids = outage_data[outage_data["current_status"] == "ONGOING"].index
        returning_duids = outage_data[outage_data["current_status"] == "RETURNING"].index
        # coal outage classified as 'New' not 'Existing' if outage started after yesterday at 10 am
        outage_starts = outage_data["outage_start"].to_list()
        new_duids = [outage_data.index[i] for i in range(len(outage_data.index)) if pd.Timestamp(outage_starts[i]) >= today - pd.DateOffset(hours=14)]

        # prepare current outages table
        current_outages = pd.DataFrame(
            columns=["name", "capacity", "outage_type", "outage_date", "existing", "expected_return"],
            index=current_duids
        )
        current_outages.index.name = "duid"

        # prepare return-to-service table
        return_to_service = pd.DataFrame(
            columns=["name", "capacity", "outage_date", "days_unavailable"],
            index=returning_duids
        )
        return_to_service.index.name = "duid"

        # populate tables

        # gather basic outage info
        for duid in current_outages.index:
            name = geninfo.at[duid, "name"]
            capacity = geninfo.at[duid, "capacity"]
            outage_start = pd.to_datetime(outage_data.at[duid, "outage_start"]).date()
            expected_return = pd.to_datetime(outage_data.at[duid, "expected_return"]).date()
            existing = False if duid in new_duids else True
            current_outages.loc[duid] = [name, capacity, pd.NA, outage_start, existing, expected_return]
        for duid in return_to_service.index:
            name = geninfo.at[duid, "name"]
            capacity = geninfo.at[duid, "capacity"]
            days_unavailable = (outage_data.at[duid, "outage_end"] - outage_data.at[duid, "outage_start"]).days + 1
            outage_start = outage_data.at[duid, "outage_start"].date()
            return_to_service.loc[duid] = [name, capacity, outage_start, days_unavailable]

        return current_duids, new_duids, returning_duids, current_outages, return_to_service

    current_duids, new_duids, returning_duids, current_outages, return_to_service = process_info(outage_data)

    # summarise outages
    print(f"{len(current_duids):,.0f} coal unit{' is' if len(current_duids) == 1 else 's are'} currently out.")
    print(f"{len(new_duids):,.0f} of these {'is a' if len(new_duids) == 1 else 'are'} new outage{'' if len(new_duids) == 1 else 's'}.")
    print(f"{len(returning_duids):,.0f} coal unit{' has' if len(returning_duids) == 1 else 's have'} returned to service today.\n")

    # iterate through DUIDs and diagnose each outage
    print("Diagnosing outages ...")
    for duid in current_duids:
        outage_type = diagnose_outage(duid, outage_data)
        current_outages.at[duid, "outage_type"] = outage_type
    print("Outage diagnoses complete.")

    # build 7-day outage table
    forecast_outages = pd.DataFrame(
        index=mtpasa["DAY"].unique(),
        columns=["total outages"]
    )
    forecast_outages.index.name = "day"

    # iterate through the 7 days to detect units with PASAAVAILABILITY = 0
    for day in forecast_outages.index:
        # extract the DUIDs of units that are out
        duids_out = mtpasa[
            (mtpasa["DAY"] == day) &
            (mtpasa["PASAAVAILABILITY"] == 0)
        ]["DUID"]
        # sum their nameplate capacity
        capacity_out = geninfo.loc[duids_out, "capacity"].sum()
        # enter data into table
        forecast_outages.at[day, "total outages"] = capacity_out

    # load report template
    print("\nWriting report to file ...", end="\r")

    # template_name = "outage_report_template.xlsx"
    template_name = "outage_report_template_v3.xlsx"
    excel_workbook = load_workbook(os.path.join(data_path, template_name))
    worksheet = excel_workbook.active

    # complete table 1
    worksheet["B3"] = f"Estimated coal generation (in MW) that will be offline based on MTPASA (4am, {today:%#d %B %Y}):"
    # fill in table
    for i, indice in enumerate(forecast_outages.index):
        column_letter = chr(ord("B") + i)
        worksheet[f"{column_letter}4"] = f"{indice:%#d %b %Y}"
        worksheet[f"{column_letter}5"] = int(forecast_outages.at[indice, 'total outages'])

    # complete table 2
    worksheet["B8"] = f"Coal units detected generating no power on {today:%#d %B %Y}:"
    
    # create map of DUID -> REGION
    df_map = pd.read_csv(os.path.join(data_path,"duid_map_july_2024.csv"))
    dfmap = df_map.to_dict()
    df_map = dict(zip(dfmap['DUID'].values(),dfmap['REGION'].values()))

    # add region column
    outage_data['Region'] = outage_data.index.map(df_map)

    # outage_by_region = # Group by 'Region' and create separate DataFrames
    outage_region_data = {key: group for key, group in outage_data.groupby('Region')}

    # table starts on row 10
    row_no = 10
    for _ in range(5):
        region = worksheet[f"B{row_no}"].value

        if region in outage_region_data.keys():
            current_duids, _, _, current_outages, return_to_service = process_info(outage_region_data[region])

            for duid in current_duids:
                outage_type = diagnose_outage(duid, outage_data)
                current_outages.at[duid, "outage_type"] = outage_type

            # sort outages by new first, then name
            current_outages.sort_values(["existing", "name"], inplace=True)

            # Loop through 
            row_no += 1

            # write to workbook
            for duid in current_outages.index:
                # insert line if required
                if row_no > 10:
                    worksheet.insert_rows(row_no)
                    worksheet.row_dimensions[row_no].height = worksheet.row_dimensions[10].height
                    # copy style/formatting to new cells
                    for col in range(8):
                        column = chr(ord("B") + col)
                        target_cell = worksheet[f"{column}{row_no}"]
                        source_cell = worksheet[f"{column}10"]
                        target_cell.font = copy(source_cell.font)
                        target_cell.border = copy(source_cell.border)
                        target_cell.fill = copy(source_cell.fill)
                        target_cell.number_format = copy(source_cell.number_format)
                        target_cell.alignment = copy(source_cell.alignment)
                # insert data into cells
                outage_date = current_outages.at[duid, "outage_date"]
                existing = current_outages.at[duid, "existing"]
                outage_type = current_outages.at[duid, "outage_type"]
                outage_days = (today - outage_date).days
                return_date = current_outages.at[duid, "expected_return"]

                if (return_date < today) | ((not existing) & (return_date == today)):
                    return_date = "Unknown"        
                worksheet[f"C{row_no}"] = geninfo.at[duid, "name"]
                worksheet[f"D{row_no}"] = "Existing" if current_outages.at[duid, "existing"] else "New"
                worksheet[f"E{row_no}"] = int(geninfo.at[duid, "capacity"])
                worksheet[f"F{row_no}"] = outage_type
                worksheet[f"G{row_no}"] = outage_days if outage_days > 0 else "New outage"
                worksheet[f"H{row_no}"] = return_date
                worksheet[f"I{row_no}"] = "Unknown" if return_date == "Unknown" else (return_date - today).days

                # make room for another duid
                row_no += 1
        else:
            # go to next region
            worksheet.delete_rows(row_no)

    thin_border = Border(
                top=Side(style='thin'), 
                bottom=Side(style='thin'))
    left_border = Border(
                left=Side(style='thin'),
                top=Side(style='thin'), 
                bottom=Side(style='thin'))
    right_border = Border(
                right=Side(style='thin'),
                top=Side(style='thin'), 
                bottom=Side(style='thin'))
    
    last_no = row_no - 1
    for col in range(8):
        column = chr(ord("B") + col)
        target_cell = worksheet[f"{column}{last_no}"]
        source_cell = worksheet[f"{column}10"]
        if col==0:
            target_cell.border = left_border
        elif col==7:
            target_cell.border = right_border
        else:
            target_cell.border = thin_border

    current_duids,_,_,current_outages,return_to_service = process_info(outage_data)

    for duid in current_duids:
        outage_type = diagnose_outage(duid, outage_data, verbose=True)
        current_outages.at[duid, "outage_type"] = outage_type

    # fill in today's total outages
    summary_row = row_no
    # total planned
    worksheet[f"D{summary_row}"] = current_outages[current_outages["outage_type"] == "Planned"]["capacity"].sum()
    # total unplanned
    worksheet[f"D{summary_row + 1}"] = current_outages[current_outages["outage_type"] == "Unplanned"]["capacity"].sum()
    # total unclear
    worksheet[f"D{summary_row + 2}"] = current_outages[current_outages["outage_type"] == "Unclear"]["capacity"].sum()
    # total outages
    worksheet[f"D{summary_row + 3}"] = current_outages["capacity"].sum()

    return_to_service_row = summary_row + 6

    # complete table 3
    worksheet[f"B{return_to_service_row}"] = f"Coal units returning to service after being offline on {today - pd.DateOffset(days=1):%#d %B %Y}:"
    # fill in table
    for i, duid in enumerate(return_to_service.index):
        row_no = return_to_service_row + i
        # insert line if required
        if i > 0:
            worksheet.insert_rows(row_no)        
            # copy style/formatting to new cells
            for col in range(3):
                column = chr(ord("B") + col)        
                target_cell = worksheet[f"{column}{row_no}"]
                source_cell = worksheet[f"{column}{return_to_service_row}"]
                target_cell.font = copy(source_cell.font)
                target_cell.border = copy(source_cell.border)
                target_cell.fill = copy(source_cell.fill)
                target_cell.number_format = copy(source_cell.number_format)
                target_cell.alignment = copy(source_cell.alignment)
        # insert data into cells
        outage_date = return_to_service.at[duid, "outage_date"]
        worksheet[f"B{row_no}"] = return_to_service.at[duid, "name"]
        worksheet[f"C{row_no}"] = int(return_to_service.at[duid, "capacity"])
        worksheet[f"D{row_no}"] = return_to_service.at[duid, "days_unavailable"]
        
    # save report
    file_name = f"outages_{today:%Y%m%d}.xlsx"
    excel_workbook.save(os.path.join(os.path.join(output_path,"COAL/"), file_name))
    
    print("Writing report to file ... complete.")
    print(f"{file_name} saved.")

    current_outages['region'] = current_outages.index.map(df_map)

    current_outages.to_csv(os.path.join(data_path,"current_outages.csv"))

    def get_future_planned(dfcp):
        '''
        feed in mtpasa df
        '''
        df = dfcp.copy()
        df = df[df['PASAAVAILABILITY']==0]

        df["consecutive_outage"] = df.groupby("DUID")["DAY"].diff().dt.days.ne(1).cumsum()

        df = df.groupby(["DUID", "consecutive_outage"]).agg(
                        start_date=("DAY", "min"),
                        end_date=("DAY", "max")).reset_index(drop=False)

        df['region'] = df['DUID'].map(df_map)
        df['outage_type'] = 'Planned' 
        df = df[['DUID','region','start_date','end_date','outage_type']]

        df.columns = ['duid','region','outage_date','expected_return','outage_type']

        return df.copy()
    
    future_planned_outages = get_future_planned(mtpasa)

    outcp = current_outages.copy()
    outcp.reset_index(inplace=True)
    outcp = outcp[['duid','region','outage_date','expected_return','outage_type']]

    outcp['outage_date'] = pd.to_datetime(outcp['outage_date'])
    outcp['expected_return'] = pd.to_datetime(outcp['expected_return'])

    all_outages = pd.concat([outcp,future_planned_outages,historic_outages],axis=0).reset_index(drop=True)
    
    all_outages.to_csv("./data/all_outages.csv",index=False)

    return all_outages.copy()

all_out = get_coal_outages()

def visualise_outages(df,today=test_date):
    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams['hatch.linewidth'] = 0.5

    today = pd.to_datetime("today").date()

    # Sort by region first, then by start date
    df.sort_values(by=["region","duid","outage_date"], inplace=True)

    df_len = len(df)

    # Assign unique indices for better region separation
    df["y_pos"] = range(df_len,0,-1)
    # df["y_pos"] = df.groupby("duid").ngroup(ascending=False) + 1


    fs = (15,max(2,df_len*0.5)) # re-scale figure according to how many outages there are

    # Set up the figure
    plt.figure(figsize=fs)
    ax = plt.gca()

    #
    ax.set_facecolor((0.9, 0.9, 0.9, 0.5))

    # Define colors by region
    regions = df["region"].unique()
    palette = sns.color_palette("Set2", n_colors=df["region"].nunique())
    region_colors = dict(zip(regions, palette))

    minOutage = df.outage_date.min()
    maxReturn = today + pd.Timedelta(days=16)

    # Assign max return date for outages expecting to be forever
    df["maxwidthCalc"] = df["expected_return"].clip(upper=pd.to_datetime(maxReturn))

    # Plot a dashed red line on today in the background
    ax.vlines(x=today,ymin=-2,ymax=df_len+2,alpha=0.4,linestyles="dashed",colors="red",zorder=1)
    ax.text(x=today,y=-1.1,s="   Today",fontsize=12,color="black",fontweight="bold")

    hatchList = []
    # Plot horizontal bars with start and end dates, thinner bars
    for i, row in df.iterrows():
        w = (row["maxwidthCalc"] - row["outage_date"]).days

        if w==0:
            # add a scatterplot instead (add one marker)
            if row["outage_type"]=="Unplanned":
                ax.scatter(
                    x = row["outage_date"],
                    y = row["y_pos"],
                    color=region_colors[row["region"]],
                    edgecolor="black",
                    marker='s',
                    s=200,
                    linewidth=1.2,
                    hatch="//")
            elif row["outage_type"]=="Unclear":
                ax.scatter(
                    x = row["outage_date"],
                    y = row["y_pos"],
                    color=region_colors[row["region"]],
                    edgecolor="black",
                    marker='s',
                    s=200,
                    linewidth=1.2,
                    hatch="xx")
            else:
                ax.scatter(
                    x = row["outage_date"],
                    y = row["y_pos"],
                    color=region_colors[row["region"]],
                    edgecolor="black",
                    marker='s',
                    s=200,
                    linewidth=1.2)
        else:
            ax.barh(
                    y=row["y_pos"], 
                    width=w, 
                    left=row["outage_date"], 
                    height=0.66,  # Reduced bar height for closer spacing
                    color=region_colors[row["region"]],
                    edgecolor="black",
                    linewidth=1.2)
            if row["outage_type"]=="Unplanned":
                hatchList.append("/")
            elif row["outage_type"]=="Unclear":
                hatchList.append("x")
            else:
                hatchList.append(False)

                
        # is it 4 days off the edge
        textposLeft = ((pd.to_datetime(maxReturn)-row["maxwidthCalc"]).days < 6) and (w < 3)

        # print(row['duid'],w,textposLeft,row["outage_date"],row["expected_return"])

        if w < 3 and not textposLeft:
            # put text to the right of the bar
            ax.text(
                row["maxwidthCalc"],  # Label on the right
                row["y_pos"],
                f"    {row['duid']}",
                va="center",
                ha="left",
                fontsize=11,
                color="black",
                fontweight="bold"
            )
        elif textposLeft:
            # it is near the end edge
            # put text to the left of the bar
            ax.text(
                row["outage_date"],  # Left label
                row["y_pos"],
                f"{row['duid']}    ",
                va="center",
                ha="right",
                fontsize=11,
                color="black",
                fontweight="bold"
            )
        else:
            ax.text(
                row["outage_date"] + pd.Timedelta(days=w/2),  # Center label
                row["y_pos"],
                f"{row['duid']}",
                va="center",
                ha="center",
                fontsize=11,
                color="#FFFFF0",
                fontweight="bold"
            )

    for i,thisbar in enumerate(ax.patches):
        # Set a different hatch for each bar
        hatch = hatchList[i]
        if hatch:
            thisbar.set_hatch(hatch)

    # Add dashed separators between regions
    region_break_end = df.groupby("region")["y_pos"].last().values[:-1]  # Get last index of each region
    for i,y in enumerate([df_len+1]+list(region_break_end)):
        ax.axhline(y=y - 0.5, color="black", linestyle="dashed", linewidth=1.2)
        ax.text(x=minOutage+pd.Timedelta(days=1),y=y,s=regions[i],
                fontsize=14,color="black",fontweight="bold")

    ax.scatter(x=minOutage+pd.Timedelta(days=1),y=-0.3,color="white",edgecolor="black",marker='s',s=100)
    ax.text(x=minOutage+pd.Timedelta(days=1),y=-0.45,s="   Planned",
            fontsize=10,color="black",fontweight="bold")
    ax.scatter(x=minOutage+pd.Timedelta(days=1),y=-0.9,color="white",edgecolor="black",marker='s',s=100,hatch="////")
    ax.text(x=minOutage+pd.Timedelta(days=1),y=-1.05,s="   Unplanned",
            fontsize=10,color="black",fontweight="bold")
    ax.scatter(x=minOutage+pd.Timedelta(days=1),y=-1.5,color="white",edgecolor="black",marker='s',s=100,hatch="xxxx")
    ax.text(x=minOutage+pd.Timedelta(days=1),y=-1.65,s="   Unclear",
            fontsize=10,color="black",fontweight="bold")

    # Format x-axis as dates

    # ALWAYS center on today +- 15 days
    ax.set_xlim(minOutage,maxReturn)
    ax.set_ylim(-2,df_len+2)
    # ax.set_ylim(-2,df["y_pos"].max())
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=4))  # Major ticks every 5 days
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))  # Format as YYYY-MM-DD
    plt.xticks(rotation=30, ha="right", fontsize=10)  # Rotate for readability

    # Remove y-axis labels since bars are annotated
    plt.yticks([])  
    plt.ylabel("")

    # Labels and title
    plt.xlabel("Date", fontsize=12)
    plt.title(f"Coal Outages (Updated on {today:%d %B %Y})", fontsize=20, fontweight="bold")

    # Legend
    handles = [plt.Line2D([0], [0], color=color, lw=6) for color in region_colors.values()]
    plt.legend(handles, region_colors.keys(), title="Region", bbox_to_anchor=(-0.105, 1), loc="upper left")

    plt.grid(axis="x", linestyle="--", alpha=0.5)
    # plt.show()

    # plt.tight_layout()
    # plt.subplots_adjust(left=0.11) 
    plt.savefig(f"./outage_{today}.png",dpi=200)

visualise_outages(all_out)