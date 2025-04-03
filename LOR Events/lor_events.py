# import dependencies
import pandas as pd
import pymssql
import matplotlib.pyplot as plt
import seaborn as sns

# set departmental style
sns.set(font="Noto Sans", font_scale=1.5, style="whitegrid")
sns.set_palette(["#083A42", "#28a399", "#801650", "#FF5E00", "#06038D", "#418FDE"])

# collect LOR market notices
def get_lor_events():
    '''Executes 'lor_events.sql', which gathers market notices that refer to actual LOR events.'''
    
    try:
        # establish connection
        with pymssql.connect(
            server="databases-plus-gr.public.f68d1dee025f.database.windows.net",
            port="3342",
            user="dept-energy",
            password="dirty-prejudge-MUTINEER-lyle-castrate",
            database="Historical-MMS"
        ) as connection:
            
            cursor = connection.cursor()

            # SQL query
            with open("./scripts/lor_events.sql", "r") as sql:
                sql_query = sql.read()
                    
            # execute query
            cursor.execute(sql_query)

            # collect results
            data = cursor.fetchall()
            columns = [row[0] for row in cursor.description]
            lor_events = pd.DataFrame(
                data=data,
                columns=columns
            )
            
            # rename columns
            lor_events.columns = ["notice_date", "region", "type", "reference"]
            
            return lor_events

    except pymssql.Error as e:
        print(f"Database error occurred: {e}")
        return None

print("Analysing AEMO market notices ...", end="\r")
lor_events = get_lor_events()
print("Analysing AEMO market notices ... complete.")

# save events to file
print("Saving historic LOR events to file ...", end="\r")
lor_events.to_csv("./data/lor_events.csv", index=False)
print("Saving historic LOR events to file ... complete.")
print("The file 'lor_events.csv' is in the 'data' sub-directory.")

# convert to monthly counts
quarterly_lors = lor_events.set_index("notice_date")
quarterly_lors = quarterly_lors.groupby("type").resample("QE").size().unstack(fill_value=0).T

# limit to past 10 years
current_year = pd.to_datetime("today").year
quarterly_lors = quarterly_lors[pd.to_datetime(f"{current_year - 10}-01-01"):]

# print and save results
quarterly_lors.index = [f"{q:%b %Y}" for q in quarterly_lors.index]
quarterly_lors.index.name = "quarter"
print("Saving quarterly summary to file ...", end="\r")
lor_events.to_csv("./data/lor_events_quarters.csv")
print("Saving quarterly summary to file ... complete.")
print("The file 'lor_events_quarters.csv' is in the 'data' sub-directory.")
print("\nQuarterly summary:")
print(quarterly_lors)

# plot monthly LOR events
fig, ax = plt.subplots(figsize=(12, 6.75), tight_layout=True)

quarterly_lors.plot(
    ax=ax,
    kind="bar",
    stacked=True,
    width=1
)
ticks = ax.get_xticks()
ax.set_xticks(ticks[::8])
ax.set_xticklabels([
    quarterly_lors.index[i][-4:] for i in range(len(quarterly_lors.index))
][::8], rotation=0)
plt.xlabel(None)
plt.ylabel("Events per quarter", fontweight="bold")
plt.legend(title=None)
plt.title(f"Quarterly LOR events since {current_year - 10}", loc="left", fontsize="large", fontweight="bold", pad=16, color="C0")
plt.figtext(
    x=.5,
    y=-.01,
    s=f"Source: AEMO market notices. Note: Data correct as of {pd.to_datetime('today'):%#d %B %Y}.",
    ha="center",
    fontsize="small"
)
plt.savefig("./data/lor_events_chart.png", bbox_inches="tight")
plt.show()