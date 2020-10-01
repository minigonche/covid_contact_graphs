# Loads the different libraries
import os
import sys
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import zscore
import matplotlib.pyplot as plt
from google.cloud import bigquery

# Gets config
import interventions.constants as con

# Starts the client
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)

# Constants
indent = con.indent
WINDOW_SIZE = 7 #days
out_path = "."

if len(sys.argv) <= 4:
    print(indent + "This scripts runs with the following args:")
    print(indent + "\t1. location_id*")
    print(indent + "\t2. report_name*")
    print(indent + "\t3. search_type* (polygon_name or polygon)")
    print(indent + "\t4. control polygon or polygon_name depending on what was chosen in the previous arg.*")
    print(indent + "\t5. treatment polygon or polygon_name depending on what was chosen in the previous arg.*")
    print(indent + "\t6. treatment_date*")
    print(indent + "\t7. start_date (Default: 2020-02-01)")
    print(indent + f"\t8. end_date (Default: Today: {datetime.datetime.now()})")
    raise Exception("Insufficient arguments. Please run again.")

# Reads the parameters from excecution
location_id  =  sys.argv[1] # location id
report_name  =  sys.argv[2] # report name
search_type = sys.argv[3] # search_type
treatment_date = sys.argv[6] # treatment_date
    
if len(sys.argv) == 8:
    start_date = sys.argv[7] 
    start_date = pd.Timestamp(datetime.datetime.strptime(start_date, '%Y-%m-%d'))
else:
    start_date = pd.Timestamp(datetime.datetime.strptime("2020-02-01", '%Y-%m-%d'))

if len(sys.argv) == 9:
    end_date = sys.argv[8]
    end_date = pd.Timestamp(datetime.datetime.strptime(end_date, '%Y-%m-%d'))
else:
    end_date = pd.Timestamp(datetime.datetime.now())

if search_type == "polygon":
    control_polygon = sys.argv[4]
    sql_0 = f"""
        SELECT *
        FROM edgelists_cities.{location_id}
        WHERE ST_DWithin(ST_GeogPoint(lon, lat), st_geogfromtext("{control_polygon}"), 1000) 
            AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """
    treatment_polygon = sys.argv[5]
    sql_1 = f"""
        SELECT *
        FROM edgelists_cities.{location_id}
        WHERE ST_DWithin(ST_GeogPoint(lon, lat), st_geogfromtext("{control_polygon}"), 1000) 
            AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """
elif search_type == "polygon_name":
    control_polygon_name = sys.argv[4]
    sql_0 = f"""
        SELECT *
        FROM edgelists_cities.{location_id}
        WHERE ST_DWithin(ST_GeogPoint(lon, lat), (SELECT geometry FROM geo.locations_geometries WHERE location_id="{control_polygon_name}"), 1000) 
            AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """
    treatment_polygon_name = sys.argv[5]
    sql_1 = f"""
        SELECT *
        FROM edgelists_cities.{location_id}
        WHERE ST_DWithin(ST_GeogPoint(lon, lat), (SELECT geometry FROM geo.locations_geometries WHERE location_id="{treatment_polygon_name}"), 1000) 
            AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """
else:
    raise Exception("Must specify either 'polygon' or 'polygon_name'.")

# export location
export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)

# Convert dates to datetime
treatment_date = pd.Timestamp(datetime.datetime.strptime(treatment_date, '%Y-%m-%d'))

# Make query for control
print(indent + f"Retreiving control contacts between {start_date} and {end_date}.")
query_job = client.query(sql_0, job_config=job_config) 

# Return the results as a pandas DataFrame
df_edges_control = query_job.to_dataframe()

if df_edges_control.empty:
    raise Exception("No points found for the given control coordinates")

# Make query for treatment 
print(indent + f"Retreiving treatment contacts between {start_date} and {end_date}.")
query_job = client.query(sql_1, job_config=job_config) 

# Return the results as a pandas DataFrame
df_edges_treatment = query_job.to_dataframe()

if df_edges_treatment.empty:
    raise Exception("No points found for the given treatment coordinates")

# df_edges_control.to_csv(os.path.join(export_folder_location, "tmp", "df_edges_control.csv"), index=False)
# df_edges_treatment.to_csv(os.path.join(export_folder_location, "tmp", "df_edges_treatment.csv"), index=False)

# Parse dates
df_edges_control["date"] = pd.to_datetime(df_edges_control["date"])   
df_edges_treatment["date"] = pd.to_datetime(df_edges_treatment["date"])

# Get contacts
df_edges_control["contacts_agg"] = 1
df_contacts_control = df_edges_control.groupby(["date", "hour"])["contacts_agg"].sum().to_frame()
df_contacts_control.reset_index(inplace=True)

# Get hour of max contact per day
df_max_hour = df_contacts_control.groupby("date")["contacts_agg"].idxmax(axis=1).to_frame()
df_min_hour = df_contacts_control.groupby("date")["contacts_agg"].idxmin(axis=1).to_frame()
df_max_hour.reset_index(inplace=True)
df_min_hour.reset_index(inplace=True)
df_max_hour["max_hour"] = df_max_hour.apply(lambda x: df_contacts_control.at[x.contacts_agg, "hour"], axis=1)
df_min_hour["min_hour"] = df_min_hour.apply(lambda x: df_contacts_control.at[x.contacts_agg, "hour"], axis=1)

df_hourly_stats_control = df_max_hour.merge(df_min_hour, on="date", how="outer").drop(columns=["contacts_agg_x", "contacts_agg_y"])
df_hourly_stats_control["week"] = df_hourly_stats_control.apply(lambda x: x.date.strftime("%V"), axis=1)
df_hourly_stats_control_avg = df_hourly_stats_control.groupby("week").mean().reset_index()

# Get contacts
df_edges_treatment["contacts_agg"] = 1
df_contacts_treatment = df_edges_treatment.groupby(["date", "hour"])["contacts_agg"].sum().to_frame()
df_contacts_treatment.reset_index(inplace=True)

print(indent + "Calculating times of most contacts")
# Get hour of max contact per day
df_max_hour = df_contacts_treatment.groupby("date")["contacts_agg"].idxmax(axis=1).to_frame()
df_min_hour = df_contacts_treatment.groupby("date")["contacts_agg"].idxmin(axis=1).to_frame()
df_max_hour.reset_index(inplace=True)
df_min_hour.reset_index(inplace=True)
df_max_hour["max_hour"] = df_max_hour.apply(lambda x: df_contacts_treatment.at[x.contacts_agg, "hour"], axis=1)
df_min_hour["min_hour"] = df_min_hour.apply(lambda x: df_contacts_treatment.at[x.contacts_agg, "hour"], axis=1)

df_hourly_stats_tratment = df_max_hour.merge(df_min_hour, on="date", how="outer").drop(columns=["contacts_agg_x", "contacts_agg_y"])
df_hourly_stats_tratment["week"] = df_hourly_stats_tratment.apply(lambda x: x.date.strftime("%V"), axis=1)
df_hourly_stats_tratment_avg = df_hourly_stats_tratment.groupby("week").mean().reset_index()

# Plot hourly stats
print(indent + "\tPlotting...")
df_plot_hourly_stats_treat = pd.melt(df_hourly_stats_tratment_avg, id_vars=['week'], value_vars=['max_hour', 'min_hour'])
df_plot_hourly_stats_ctrl = pd.melt(df_hourly_stats_control_avg, id_vars=['week'], value_vars=['max_hour', 'min_hour'])

fig, axs = plt.subplots(2)
fig.suptitle('Hora de mayor y menor numero de contactos')
sns.lineplot(data=df_plot_hourly_stats_treat, x="week", y="value", hue="variable", palette="Set2", ax=axs[0])
sns.lineplot(data=df_plot_hourly_stats_ctrl, x="week", y="value", hue="variable", palette="Set2", ax=axs[1])
for ax in fig.get_axes():
    ax.vlines(x=treatment_date.strftime("%V"), ymin=-1, ymax=20, label="Intervención", color="grey", linestyles='dashed')
    ax.set(xlabel='Semana', ylabel='Hora')
    ax.label_outer()

print(indent + "\tExporting...")
plt.savefig(os.path.join(export_folder_location, f"hourly_stats_{control_polygon_name}-{treatment_polygon_name}.png"))

print(indent + f"Calculating percentage change in number of contacts using the week of {start_date} as baseline")
# Baseline is calculated over a week

# Remove outliers
print(indent + "\tRemoving outliers...")
y = df_contacts_control['contacts_agg']
tot_c = df_contacts_control.shape[0]
removed_outliers = y.between(y.quantile(.05), y.quantile(.95))
df_contacts_control = df_contacts_control[removed_outliers]
new_c = df_contacts_control.shape[0]
y = df_contacts_treatment['contacts_agg']
tot_t = df_contacts_treatment.shape[0]
removed_outliers = y.between(y.quantile(.05), y.quantile(.95))
df_contacts_treatment = df_contacts_treatment[removed_outliers]
new_t = df_contacts_treatment.shape[0]
print(indent + f"\t\t{tot_c - new_c} datapoints removed from control dataset.")
print(indent + f"\t\t{tot_t - new_t} datapoints removed from treatment dataset.")

df_contacts_control = df_contacts_control.groupby("date")["contacts_agg"].sum().to_frame().reset_index()
df_contacts_treatment = df_contacts_treatment.groupby("date")["contacts_agg"].sum().to_frame().reset_index()

baseline_control = df_contacts_control.loc[df_contacts_control["date"] < start_date + datetime.timedelta(days = WINDOW_SIZE)]["contacts_agg"].mean()
baseline_treatment = df_contacts_treatment.loc[df_contacts_treatment["date"] < start_date + datetime.timedelta(days = WINDOW_SIZE)]["contacts_agg"].mean()

df_contacts_control["percentage_change_ctrl"] = df_contacts_control["contacts_agg"].subtract(baseline_control).divide(baseline_control)
df_contacts_treatment["percentage_change_trtm"] = df_contacts_treatment["contacts_agg"].subtract(baseline_treatment).divide(baseline_treatment)

print(indent + "\tSaving control and treatment dataset.")
df_contacts_control.to_csv(os.path.join(export_folder_location, f"contacts_{control_polygon_name}.csv"), index=False)
df_contacts_treatment.to_csv(os.path.join(export_folder_location, f"contacts_{treatment_polygon_name}.csv"), index=False)

df_contacts = df_contacts_control.merge(df_contacts_treatment, on="date", how="outer").drop(columns=["contacts_agg_x", "contacts_agg_y"])

# Plot contact change
print(indent + "\tPlotting...")
df_contacts_plot = pd.melt(df_contacts, id_vars=['date'], value_vars=['percentage_change_ctrl', 'percentage_change_trtm'])

fig1, ax1 = plt.subplots()
plt.figure(figsize=(15,8))
sns.lineplot(data=df_contacts_plot, x="date", y="value", hue="variable", palette="Set2")
plt.suptitle("Cambio porcentual de contactos", fontsize=18)
plt.title(f"Con respecto a la semana del {start_date.strftime('%Y-%m-%d')}", fontsize=10)
plt.vlines(x=treatment_date, ymin=-1, ymax=df_contacts_plot.value.max(), label="Intervención", color="grey", linestyles='dashed')
plt.xlabel("Fecha")
plt.ylabel("Cambio porcentual")

print(indent + "\tExporting...")
plt.savefig(os.path.join(export_folder_location, f"percent_change_contacts_{control_polygon_name}-{treatment_polygon_name}.png"))
