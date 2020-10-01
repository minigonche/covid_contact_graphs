import os
import sys
import datetime
import numpy as np
import pandas as pd

import utils.excecution_functions as ef

# Constants
scripts = {"single":["contacts_single_polygon.py", "graph_attributes_single_polygon.py"],
           "double":["contacts.py", "graph_attributes.py"]}   

base_path = os.path.dirname(os.path.realpath(__file__))

interventions = os.path.join(base_path, "intervenciones.csv")
scripts_location = os.path.join(base_path, "scripts")

df_interventions = pd.read_csv(interventions)

for i in df_interventions.index:
    location_id = df_interventions.at[i, "location_id"]
    print(f"Running intervention analysis for {location_id}.") 
    search_type = "polygon_name"
    report_name = df_interventions.at[i, "report_name"]
    control_polygon_name = df_interventions.at[i, "control_polygon"]
    treatment_polygon_name = df_interventions.at[i, "treatment_polygon"]
    treatment_date = df_interventions.at[i, "treatment_date"]
    start_date = df_interventions.at[i, "start_date"]
    end_date = df_interventions.at[i, "end_date"]
    
    if pd.isnull(start_date):
        start_date = "2020-02-01"
    if pd.isnull(end_date):
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    if pd.isnull(treatment_polygon_name):
        print(f"Running scripts for {control_polygon_name} with treatment date {treatment_date}.")
        script_names = scripts["single"] 
        exec_parameters = f"{location_id} {report_name} {search_type} {control_polygon_name} {treatment_date} {start_date} {end_date}"
    else:
        print(f"Running scripts for {control_polygon_name} and {treatment_polygon_name} with treatment date {treatment_date}.")
        script_names = scripts["double"] 
        exec_parameters = f"{location_id} {report_name} {search_type} {control_polygon_name} {treatment_polygon_name} {treatment_date} {start_date} {end_date}"

    for s in script_names:
        print(f" Running {s}")
        ef.excecute_script(scripts_location, s, "python", exec_parameters)