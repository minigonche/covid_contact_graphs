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

# Local funcitons
import utils.general_functions as ge

# Gets config
import config_constants as con
import constants as const

# Starts the client
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)

# Constants
indent = const.indent
WINDOW_SIZE = 7 #days
COLOR_CTRL = "#79c5b4"
COLOR_TRT = "#324592"
COLOR_HIHGLIGH = '#ff8c65'
control_flag = False
remove_outliers = False
DIFFDIFF_CODE = const.diffdiff_codes["contacts"]


if len(sys.argv) <= 4:
    print(indent + "This scripts runs with the following args:")
    print(indent + "\t1. location_id*")
    print(indent + "\t2. report_name*")
    print(indent + "\t3. treatment polygon*")
    print(indent + "\t4. control polygon*")
    print(indent + "\t5. treatment_date*")
    print(indent + "\t6. start_date (Default: 2020-02-01)")
    print(indent + f"\t7. end_date (Default: Today: {datetime.datetime.now()})")
    raise Exception("Insufficient arguments. Please run again.")

# Reads the parameters from excecution
location_id  =  sys.argv[1] # location id
report_name  =  sys.argv[2] # report name
treatment_polygon_name = sys.argv[3] # treatment_polygon
control_polygon_name = sys.argv[4] # control_polygon
treatment_date = sys.argv[5] # treatment_date

# Get dataset details
sql = f"""
    SELECT *
    FROM geo.locations_geometries 
    WHERE location_id="{treatment_polygon_name}"
    """

query_job = client.query(sql, job_config=job_config) 
df_database = query_job.to_dataframe()
if df_database.empty:
    raise Exception(f"No dataset matches the location_id {treatment_polygon_name}.")
if df_database.shape[0] > 1:
    raise Exception(f"Multiple datasets found for location_id {treatment_polygon_name}.")
data_set_trtm = df_database.at[0, "dataset"]
data_name_trtm = df_database.at[0, "name"]
data_name_trtm = ge.clean_for_publication(data_name_trtm)

if len(sys.argv) == 8:
    start_date = sys.argv[6] 
    start_date = pd.Timestamp(datetime.datetime.strptime(start_date, '%Y-%m-%d'))
else:
    start_date = pd.Timestamp(datetime.datetime.strptime("2020-02-01", '%Y-%m-%d'))

if len(sys.argv) == 8:
    end_date = sys.argv[7]
    end_date = pd.Timestamp(datetime.datetime.strptime(end_date, '%Y-%m-%d'))
else:
    end_date = pd.Timestamp(datetime.datetime.now())

# export location
export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)
    
# Convert dates to datetime
treatment_date = pd.Timestamp(datetime.datetime.strptime(treatment_date, '%Y-%m-%d'))

sql_0 = f"""
    SELECT *
    FROM {data_set_trtm}.{treatment_polygon_name} 
    WHERE date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """

# Make query for treatment
print(indent + f"Retreiving treatment contacts between {start_date} and {end_date}.")
query_job = client.query(sql_0, job_config=job_config) 

# Return the results as a pandas DataFrame
df_edges_treatment = query_job.to_dataframe()

if df_edges_treatment.empty:
    raise Exception("No points found for the given treatment coordinates")

df_edges_treatment["date"] = pd.to_datetime(df_edges_treatment["date"])    

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
df_hourly_stats_tratment["week"] = df_hourly_stats_tratment.apply(lambda x: int(x.date.strftime("%V")), axis=1)
d = range(df_hourly_stats_tratment["week"].min(), int(end_date.strftime("%V")))
d = [k for k in d if k >= int(treatment_date.strftime("%V"))]
df_hourly_stats_tratment_avg = df_hourly_stats_tratment.groupby("week").mean().reset_index()
df_hourly_stats_tratment_avg.rename(columns={"max_hour":"Hora máxima", "min_hour":"Hora mínima"}, inplace=True)
df_plot_hourly_stats_treat = pd.melt(df_hourly_stats_tratment_avg, id_vars=['week'], value_vars=['Hora máxima', 'Hora mínima'])

if control_polygon_name != "None":
    control_flag = True
    # export location
    export_folder_location_diffdiff = os.path.join(export_folder_location, f"{treatment_polygon_name}-{control_polygon_name}", "diff-diff")
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}-{control_polygon_name}", "contacts")    
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 
else:
    # export location
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}", "contacts")
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 
        
if control_flag:
    sql = f"""
    SELECT *
    FROM geo.locations_geometries 
    WHERE location_id="{control_polygon_name}"
    """

    query_job = client.query(sql, job_config=job_config) 
    df_database = query_job.to_dataframe()
    if df_database.empty:
        raise Exception(f"No dataset matches the location_id {control_polygon_name}.")
    if df_database.shape[0] > 1:
        raise Exception(f"Multiple datasets found for location_id {control_polygon_name}.")
    data_set_ctrl = df_database.at[0, "dataset"]
    data_name_ctrl = df_database.at[0, "name"]
    data_name_ctrl = ge.clean_for_publication(data_name_ctrl)
    
    sql_1 = f"""
        SELECT *
        FROM {data_set_ctrl}.{control_polygon_name} 
        WHERE date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
        """
    
    # Make query for control 
    print(indent + f"Retreiving control contacts between {start_date} and {end_date}.")
    query_job = client.query(sql_1, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df_edges_control = query_job.to_dataframe()

    if df_edges_treatment.empty:
        raise Exception("No points found for the given treatment coordinates")

    df_edges_control["date"] = pd.to_datetime(df_edges_control["date"])

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
    df_hourly_stats_control["week"] = df_hourly_stats_control.apply(lambda x: int(x.date.strftime("%V")), axis=1)
    d = range(df_hourly_stats_control["week"].min(), int(end_date.strftime("%V")))
    d = [k for k in d if k >= int(treatment_date.strftime("%V"))]
    df_hourly_stats_control_avg = df_hourly_stats_control.groupby("week").mean().reset_index()
    df_hourly_stats_control_avg.rename(columns={"max_hour":"Hora máxima", "min_hour":"Hora mínima"}, inplace=True)
    df_plot_hourly_stats_ctrl = pd.melt(df_hourly_stats_control_avg, id_vars=['week'], value_vars=['Hora máxima', 'Hora mínima'])
    
    titles = ["tratamiento", "control"]
    fig, axs = plt.subplots(2, figsize=(10,5), sharex=True)
    fig.suptitle('Hora de mayor y menor numero de contactos')
    sns.lineplot(data=df_plot_hourly_stats_treat, x="week", y="value", hue="variable", palette="Set2", ax=axs[0])
    sns.lineplot(data=df_plot_hourly_stats_ctrl, x="week", y="value", hue="variable", palette="Set2", ax=axs[1])
    for i in range(len(fig.get_axes())):
        ax = fig.get_axes()[i]
        ax.set_title(titles[i])
        ax.vlines(x=int(treatment_date.strftime("%V")), ymin=-1, ymax=20, label="Intervención", color=COLOR_HIHGLIGH, linestyles='dashed')
        ax.fill_between(d, -1, 20, facecolor=COLOR_HIHGLIGH, alpha = 0.25) 
        ax.set(xlabel='Semana', ylabel='Hora')
        ax.label_outer()
    print(indent + "\tExporting...")
    plt.savefig(os.path.join(export_folder_location,"hourly_stats.png"))   
    
# Remove outliers
if remove_outliers:
    print(indent + "\tRemoving outliers...")
    y = df_contacts_treatment['contacts_agg']
    tot_t = df_contacts_treatment.shape[0]
    removed_outliers = y.between(y.quantile(.05), y.quantile(.95))
    df_contacts_treatment = df_contacts_treatment[removed_outliers]
    new_t = df_contacts_treatment.shape[0]
    print(indent + f"\t\t{tot_t - new_t} datapoints removed from treatment dataset.")

print(indent + f"Calculating percentage change in number of contacts using the week of {start_date} as baseline")
if start_date < df_contacts_treatment.date.min():
    start_date = df_contacts_treatment.date.min()
# Baseline is calculated over a week
df_contacts_treatment = df_contacts_treatment.groupby("date")["contacts_agg"].sum().to_frame().reset_index()
baseline_treatment = df_contacts_treatment.loc[df_contacts_treatment["date"] < \
                                                   start_date + datetime.timedelta(days = WINDOW_SIZE)]["contacts_agg"].mean()
df_contacts_treatment["percentage_change_trtm"] = df_contacts_treatment["contacts_agg"].subtract(baseline_treatment).divide(baseline_treatment)

print(indent + "\tSaving control and treatment dataset.")
df_contacts_treatment.to_csv(os.path.join(export_folder_location, "contacts.csv"), index=False)
df_contacts = df_contacts_treatment.copy()
    
if control_flag == False:    
    # Plot hourly stats
    print(indent + "\tPlotting...")
    fig, ax = plt.subplots()
    fig.suptitle('Hora de mayor y menor numero de contactos')
    sns.lineplot(data=df_plot_hourly_stats_treat, x="week", y="value", hue="variable", palette="Set2", ax=ax)
    ax.vlines(x=int(treatment_date.strftime("%V")), ymin=-1, ymax=20, label="Intervención", color=COLOR_HIHGLIGH, linestyles='dashed')
    ax.fill_between(d, -1, 20, facecolor=COLOR_HIHGLIGH, alpha = 0.25) 
    ax.set(xlabel='Semana', ylabel='Hora')
    print(indent + "\tExporting...")
    plt.savefig(os.path.join(export_folder_location, f"hourly_stats.png"))

    # Plot contact change
    print(indent + "\tPlotting...")
    d = pd.date_range(start_date, end_date)
    d = [k for k in d if k >= treatment_date]
    maxy = df_contacts.percentage_change_trtm.max()
    miny = df_contacts.percentage_change_trtm.min()
    column_trtm = f"Cambio porcentual {data_name_trtm}"
    df_contacts.rename(columns={"percentage_change_trtm": column_trtm}, inplace=True)
    df_contacts_plot = pd.melt(df_contacts, id_vars=['date'], value_vars=[column_trtm])
    fig1, ax1 = plt.subplots()
    plt.figure(figsize=(15,8))
    sns.lineplot(data=df_contacts_plot, x="date", y="value", hue="variable", palette="Set2")
    plt.suptitle(f"Cambio porcentual de contactos", fontsize=18)
    plt.title(f"Con respecto a la semana del {start_date.strftime('%Y-%m-%d')}", fontsize=10)
    plt.fill_between(d, miny, maxy, facecolor=COLOR_HIHGLIGH, alpha = 0.25)
    plt.vlines(x=treatment_date, ymin=miny, ymax=maxy, label="Intervención", color=COLOR_HIHGLIGH, linestyles='dashed')
    plt.xlabel("Fecha")
    plt.ylabel("Cambio porcentual")
    print(indent + "\tExporting...")
    plt.savefig(os.path.join(export_folder_location, f"percent_change_contacts.png"))

else:
    if remove_outliers:
        # Remove outliers
        print(indent + "\tRemoving outliers...")
        y = df_contacts_control['contacts_agg']
        tot_c = df_contacts_control.shape[0]
        removed_outliers = y.between(y.quantile(.05), y.quantile(.95))
        df_contacts_control = df_contacts_control[removed_outliers]
        new_c = df_contacts_control.shape[0]
        print(indent + f"\t\t{tot_c - new_c} datapoints removed from control dataset.")
    
    if start_date < df_contacts_control.date.min():
        start_date = df_contacts_control.date.min()
    
    df_contacts_control = df_contacts_control.groupby("date")["contacts_agg"].sum().to_frame().reset_index()
    baseline_control = df_contacts_control.loc[df_contacts_control["date"] < start_date + datetime.timedelta(days = WINDOW_SIZE)]["contacts_agg"].mean()
    df_contacts_control["percentage_change_ctrl"] = df_contacts_control["contacts_agg"].subtract(baseline_control).divide(baseline_control)
    df_contacts = df_contacts_control.merge(df_contacts_treatment, on="date", how="outer").drop(columns=["contacts_agg_x", "contacts_agg_y"])
    
    # Write csv for diff-diff
    print(indent + f"Writing diff-diff file.")
    ctrl_column = f"{DIFFDIFF_CODE}-1-contacts_percentage_change-ctrl"
    trtm_column = f"{DIFFDIFF_CODE}-1-contacts_percentage_change-trtm"
    df_contacts.rename(columns={"percentage_change_ctrl":ctrl_column, \
                                "percentage_change_trtm":trtm_column}).to_csv(
        os.path.join(export_folder_location_diffdiff, f"contacts.csv"), index=False)
    
    # Plot contact change
    print(indent + "\tPlotting...")
    d = pd.date_range(start_date, end_date)
    d = [k for k in d if k >= treatment_date]

    maxy = df_contacts.percentage_change_trtm.max()
    miny = df_contacts.percentage_change_trtm.min()
    column_ctrl = f"Cambio porcentual {data_name_ctrl} (control)"
    column_trtm = f"Cambio porcentual {data_name_trtm} (tratamiento)"
    df_contacts.rename(columns={"percentage_change_trtm": column_trtm, 
                                "percentage_change_ctrl": column_ctrl}, inplace=True)
    df_contacts_plot = pd.melt(df_contacts, id_vars=['date'], value_vars=[column_ctrl, column_trtm])

    fig1, ax1 = plt.subplots()
    plt.figure(figsize=(15,8))
    sns.lineplot(data=df_contacts_plot, x="date", y="value", hue="variable", palette="Set2")
    plt.suptitle("Cambio porcentual de contactos", fontsize=18)
    plt.title(f"Con respecto a la semana del {start_date.strftime('%Y-%m-%d')}", fontsize=10)
    plt.fill_between(d, miny, maxy, facecolor=COLOR_HIHGLIGH, alpha = 0.25)
    plt.vlines(x=treatment_date, ymin=miny, ymax=maxy, label="Intervención", color=COLOR_HIHGLIGH, linestyles='dashed')
    plt.xlabel("Fecha")
    plt.ylabel("Cambio porcentual")

    print(indent + "\tExporting...")
    plt.savefig(os.path.join(export_folder_location, f"percent_change_contacts.png"))