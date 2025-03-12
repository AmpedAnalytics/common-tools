# import dependencies
from copy import copy
from openpyxl import load_workbook
import os
import pandas as pd

# import local scripts
from outage_scripts import (
    get_coal_availability,
    diagnose_outage,
    get_outage_data,
    get_geninfo
)

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

    mtpasa = get_coal_availability(
        coal_duids,
        today + pd.DateOffset(days=1)
    )

    outage_data = get_outage_data(
        coal_duids,
        today
    )

    df_map = pd.read_csv(os.path.join(data_path,"./duid_map_july_2024.csv"))
    dfmap = df_map.to_dict()
    df_map = dict(zip(dfmap['DUID'].values(),dfmap['REGION'].values()))

    outage_data['Region'] = outage_data.index.map(df_map)

    # outage_by_region = # Group by 'Region' and create separate DataFrames
    dfs = {key: group for key, group in outage_data.groupby('Region')}

    print(dfs)

    
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

    template_name = "outage_report_template.xlsx"
    excel_workbook = load_workbook(os.path.join(data_path, template_name))
    worksheet = excel_workbook.active

    # complete table 1
    worksheet["B3"] = f"Estimated coal generation (in MW) that will be offline based on MTPASA (4am, {today:%#d %B %Y}):"
    # fill in table
    for i, indice in enumerate(forecast_outages.index):
        column_letter = chr(ord("B") + i)
        worksheet[f"{column_letter}4"] = f"{indice:%#d %b %Y}"
        worksheet[f"{column_letter}5"] = int(forecast_outages.at[indice, 'total outages'])


    #!!! --- CHANGED THIS SECTION BELOW --- !!!#

    # complete table 2
    worksheet["B8"] = f"Coal units detected generating no power on {today:%#d %B %Y}:"
    # sort outages by new first, then name
    current_outages.sort_values(["existing", "name"], inplace=True)
    # fill in table
    for i, duid in enumerate(current_outages.index):
        row_no = 10 + i
        # insert line if required
        if row_no > 10:
            worksheet.insert_rows(row_no)
            worksheet.row_dimensions[row_no].height = worksheet.row_dimensions[10].height
            # copy style/formatting to new cells
            for col in range(7):
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
        worksheet[f"B{row_no}"] = geninfo.at[duid, "name"]
        worksheet[f"C{row_no}"] = "Existing" if current_outages.at[duid, "existing"] else "New"
        worksheet[f"D{row_no}"] = int(geninfo.at[duid, "capacity"])
        worksheet[f"E{row_no}"] = outage_type
        worksheet[f"F{row_no}"] = outage_days if outage_days > 0 else "New outage"
        worksheet[f"G{row_no}"] = return_date
        worksheet[f"H{row_no}"] = "Unknown" if return_date == "Unknown" else (return_date - today).days
    # fill in today's total outages
    return_to_service_row = 18 + max(1, len(current_duids))
    # total planned
    worksheet[f"D{return_to_service_row - 8}"] = current_outages[current_outages["outage_type"] == "Planned"]["capacity"].sum()
    # total unplanned
    worksheet[f"D{return_to_service_row - 7}"] = current_outages[current_outages["outage_type"] == "Unplanned"]["capacity"].sum()
    # total unclear
    worksheet[f"D{return_to_service_row - 6}"] = current_outages[current_outages["outage_type"] == "Unclear"]["capacity"].sum()
    # total outages
    worksheet[f"D{return_to_service_row - 5}"] = current_outages["capacity"].sum()

     #!!! ---        --- !!!#

    # complete table 3
    worksheet[f"B{return_to_service_row - 2}"] = f"Coal units returning to service after being offline on {today - pd.DateOffset(days=1):%#d %B %Y}:"
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
    #excel_workbook.save(os.path.join(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '../..', 'OUTPUT', 'COAL')), file_name))
    
    print("Writing report to file ... complete.")
    print(f"{file_name} saved.")