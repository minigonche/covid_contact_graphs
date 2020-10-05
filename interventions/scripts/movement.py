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

# Get translation table
sql = f"""
    SELECT *
    FROM graph_attributes.attribute_names 
    """

query_job = client.query(sql, job_config=job_config) 
df_translations = query_job.to_dataframe()

df_translations.set_index(["type", "attribute_name"], inplace=True)

def translate(attr_name):
    translation = df_translations.iloc[(df_translations.index.get_level_values('type') == "graph_movement") \
                                       & (df_translations.index.get_level_values('attribute_name') == attr_name)]["translated_name"]
    return translation.values[0]

def get_yaxis_name(attr_name):
    yaxis_name = df_translations.iloc[(df_translations.index.get_level_values('type') == "graph_movement") \
                                       & (df_translations.index.get_level_values('attribute_name') == attr_name)]["yaxis_name"]
    return yaxis_name.values[0]

COLOR_CTRL = "#79c5b4"
COLOR_TRT = "#324592"
COLOR_HIHGLIGH = '#ff8c65'
control_flag = False

DIFFDIFF_CODE = const.diffdiff_codes["movement"]



if len(sys.argv) <= 5:
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
treatment_polygon_name = sys.argv[3] # treatment_polygon_name
control_polygon_name = sys.argv[4] # control_polygon_name

treatment_date = sys.argv[5] # treatment_date
    
# export location
export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    
    
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

# Convert dates to datetime
treatment_date = pd.Timestamp(datetime.datetime.strptime(treatment_date, '%Y-%m-%d'))

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


sql_0 = f"""
    SELECT *
    FROM graph_attributes.graph_movement
    WHERE location_id="{treatment_polygon_name}" 
        AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """

# Make query for treatment
print(indent + f"Retreiving treatment movement between {start_date} and {end_date}.")
query_job = client.query(sql_0, job_config=job_config) 

# Return the results as a pandas DataFrame
df_mov_treatment = query_job.to_dataframe()

if df_mov_treatment.empty:
    raise Exception("No movement found for the given treatment.")
    
df_mov_treatment["date"] = pd.to_datetime(df_mov_treatment["date"])
df_mov_treatment.sort_values(by="date", inplace=True)

if control_polygon_name != "None":
    control_flag = True
    
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
    
    # export location
    export_folder_location_diffdiff = os.path.join(export_folder_location, f"{treatment_polygon_name}-{control_polygon_name}", "diff-diff")    
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}-{control_polygon_name}", "movement")
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)
    if not os.path.exists(export_folder_location_diffdiff):
        os.makedirs(export_folder_location_diffdiff)
    
else:
    # export location
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}", "movement")
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 

attrs = list(set(df_mov_treatment.columns) - set(["location_id", "date"]))

if control_flag:
    sql_1 = f"""
        SELECT *
        FROM graph_attributes.graph_movement
        WHERE location_id="{control_polygon_name}"  
            AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """
    # Make query for control
    print(indent + f"Retreiving control movement between {start_date} and {end_date}.")
    query_job = client.query(sql_1, job_config=job_config) 

    df_mov_control = query_job.to_dataframe()
    
    if df_mov_control.empty:
        print("WARNING: No movement found for the given control. Running script only for treatment polygon")
        control_flag = False
    else:        
        df_mov_control["date"] = pd.to_datetime(df_mov_control["date"])
        df_mov_control.sort_values(by="date", inplace=True)
            
        attrs = set(df_mov_treatment.columns).intersection(set(df_mov_control.columns))
        attrs = list(attrs - set(["location_id", "date"]))
        
        # Write csv for diff-diff
        print(indent + f"Writing diff-diff file.")
        rename_cols_ctrl = {}
        rename_cols_trtm = {}
        for i in range(len(attrs)):
            attr = attrs[i] 
            ctrl_column = f"{DIFFDIFF_CODE}-{i}-{attr}-ctrl"
            trtm_column = f"{DIFFDIFF_CODE}-{i}-{attr}-trtm"
            rename_cols_ctrl[attr] = ctrl_column
            rename_cols_trtm[attr] = trtm_column
        df_diff_diff = df_mov_control.rename(columns=rename_cols_ctrl) \
            .merge(df_mov_treatment.rename(columns=rename_cols_trtm), on="date", how="outer")
        df_diff_diff.drop(columns=["location_id_x", "location_id_y"], inplace=True)
        df_diff_diff.to_csv(os.path.join(export_folder_location_diffdiff, f"movement.csv"), index=False)
        
    


print(indent + f"Plotting...")
for attr in attrs:    
    print(indent + f"\t{attr}")
    fig, ax = plt.subplots() 
    ax.set_title(translate(attr))
    ax.set_xlabel("Fecha")
    ax.set_ylabel(get_yaxis_name(attr))
    maxy = df_mov_treatment[attr].max()
    miny = df_mov_treatment[attr].min()
    d = pd.date_range(treatment_date, end_date).values
    fig.set_figheight(5)
    fig.set_figwidth(15)
    ax.plot(df_mov_treatment["date"], df_mov_treatment[attr], linewidth=1, color=COLOR_TRT, label=f"{data_name_trtm} (tratamiento)")
    fig_name = f"{attr}.png"
    if control_flag:
        maxy = max(df_mov_control[attr].max(), df_mov_treatment[attr].max())
        miny = min(df_mov_control[attr].min(), df_mov_treatment[attr].min())
        ax.plot(df_mov_control["date"], df_mov_control[attr], linewidth=1, color=COLOR_CTRL, label=f"{data_name_ctrl} (control)")
    ax.fill_between(d, maxy, miny, facecolor=COLOR_HIHGLIGH, alpha = 0.25) 
    ax.axvline(treatment_date, linestyle="--", color=COLOR_HIHGLIGH, linewidth=1)
    ax.legend()
    fig.savefig(os.path.join(export_folder_location,fig_name))
print(indent + "Exporting...")

    



