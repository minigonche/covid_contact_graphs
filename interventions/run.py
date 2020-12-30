import os
import sys
import datetime
import numpy as np
import pandas as pd

import utils.excecution_functions as ef
import constants as con

# Constants
scripts = ["movement.py", "graph_attributes.py", "contacts.py"] 
# scripts = ["graph_attributes.py"]
base_path = os.path.dirname(os.path.realpath(__file__))

interventions = os.path.join(base_path, "intervenciones.csv")
scripts_location = os.path.join(base_path, "scripts")

df_interventions = pd.read_csv(interventions)

for i in df_interventions.index:
    location_id = df_interventions.at[i, "location_id"]
    report_name = df_interventions.at[i, "report_name"]
    control_polygon_name = df_interventions.at[i, "control_polygon"]
    treatment_polygon_name = df_interventions.at[i, "treatment_polygon"]
    treatment_date = df_interventions.at[i, "treatment_date"]
    treatment_length = df_interventions.at[i, "treatment_length"]
    start_date = df_interventions.at[i, "start_date"]
    end_date = df_interventions.at[i, "end_date"] 
    if pd.isnull(start_date):
        start_date = "2020-02-01"
    if pd.isnull(end_date):
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    print(f"Running intervention analysis for {control_polygon_name}, between: {start_date} and {end_date}.")
    exec_parameters = f"{location_id} {report_name} {treatment_polygon_name} {control_polygon_name} {treatment_date} {treatment_length} {start_date} {end_date}"
        
    for s in scripts:
        print(f" Running {s}")
        ef.excecute_script(scripts_location, s, "python", exec_parameters)

    if control_polygon_name != "None":    
        export_folder_location = os.path.join(con.reports_folder_location, \
                                              report_name, con.figure_folder_name, \
                                              f"{treatment_polygon_name}-{control_polygon_name}", "diff-diff")
        print(f" Running diff-diff for {treatment_polygon_name}.")
        print(f"\tUsing {control_polygon_name} as control and {treatment_date} as treatment date.")
        exec_parameters = f"{export_folder_location} {treatment_date} {treatment_length} {end_date}"
        ef.excecute_script(scripts_location, "diff-diff.py", "python", exec_parameters) 
    