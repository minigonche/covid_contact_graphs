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
import config_constants as con
import constants as const

# Starts the client
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)

# Constants
indent = const.indent
WINDOW_SIZE = 7 #days
translate_title = {"all_devices": "Número de dispositivos", 
            "devices_with_movement": "Número de dispositivos con movimiento",
            "devices_stayed_in": "Número de dispositivos que no salieron del polígono",
            "devices_traveled_outside": "Número de dispositivos que salieron del polígono",
            "percentaga_stayed_in": "Porcentaje de dispositivos que no salieron del polígono",
            "percentaga_treveled_outside": "Porcentaje de dispositivos que salieron del polígono",
            "all_movement": "Movimiento total",
            "all_movement_avg": "Movimiento total promedio",
            "inner_movement": "Movimiento interno",
            "inner_movement_avg": "Movimiento interno promedio",
            "outer_movement": "Movimiento externo",
            "outer_movement_avg": "Movimiento externo promedio"}

translate_ylabel = {"all_devices": "Número de dispositivos", 
            "devices_with_movement": "Número de dispositivos",
            "devices_stayed_in": "Número de dispositivos",
            "devices_traveled_outside": "Número de dispositivos",
            "percentaga_stayed_in": "Porcentaje (%)",
            "percentaga_treveled_outside": "Porcentaje (%)",
            "all_movement": "Movimiento",
            "all_movement_avg": "Movimiento promedio",
            "inner_movement": "Movimiento",
            "inner_movement_avg": "Movimiento promedio",
            "outer_movement": "Movimiento",
            "outer_movement_avg": "Movimiento promedio"}

COLOR_CTRL = "#79c5b4"
COLOR_TRT = "#324592"
COLOR_HIHGLIGH = '#ff8c65'
control_flag = False

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
    
if len(sys.argv) == 7:
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
    # export location
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}-{control_polygon_name}", "mov")
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 
else:
    # export location
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}", "mov")
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 


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
    
attrs = list(set(df_mov_treatment.columns) - set(["location_id", "date"]))

print(indent + f"Plotting...")
for attr in attrs:    
    print(indent + f"\t{attr}")
    fig, ax = plt.subplots() 
    ax.set_title(translate_title[attr])
    ax.set_xlabel("Fecha")
    ax.set_ylabel(translate_ylabel[attr])
    maxy = df_mov_treatment[attr].max()
    miny = df_mov_treatment[attr].min()
    d = pd.date_range(treatment_date, end_date).values
    fig.set_figheight(5)
    fig.set_figwidth(15)
    ax.plot(df_mov_treatment["date"], df_mov_treatment[attr], linewidth=1, color=COLOR_TRT, label=f"{treatment_polygon_name} (tratamiento)")
    fig_name = f"{attr}.png"
    if control_flag:
        maxy = max(df_mov_control[attr].max(), df_mov_treatment[attr].max())
        miny = min(df_mov_control[attr].min(), df_mov_treatment[attr].min())
        ax.plot(df_mov_control["date"], df_mov_control[attr], linewidth=1, color=COLOR_CTRL, label=f"{control_polygon_name} (control)")
    ax.fill_between(d, maxy, miny, facecolor=COLOR_HIHGLIGH, alpha = 0.25) 
    ax.axvline(treatment_date, linestyle="--", color=COLOR_HIHGLIGH, linewidth=1)
    ax.legend()
    fig.savefig(os.path.join(export_folder_location,fig_name))
print(indent + "Exporting...")

    



