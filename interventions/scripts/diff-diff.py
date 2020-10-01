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

# Starts the client
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)

# Constants
indent = "  "
WINDOW_SIZE = 7 #days
input_path = "."
out_path = "."

# Reads the parameters from excecution
location_id  =  sys.argv[1] # location id

if len(sys.argv) < 3:
    raise Exception("Please enter at least one treatment date.")
    
i = 2
treatment_dates = []
while i < len(sys.argv):
    try:
        window = sys.argv[i].split(",")
        start = window[0].strip()
        start = pd.Timestamp(datetime.datetime.strptime(start, '%Y-%m-%d'))
        end = window[1].strip()
        end = pd.Timestamp(datetime.datetime.strptime(end, '%Y-%m-%d'))
        date = (start, end)
    except:
        raise Exception("Please input treatment window as 'YYYY-mm-dd,YYYY-mm-dd'.")
    
    treatment_dates.append(date)
    i += 1

print(indent + f"Calculating diff-diff for {location_id}. {len(treatment_dates)} found.")
with open(os.path.join(out_path, "diff-diff.csv", 'w') as f):
    f.write("window\ttype\tdiff-diff\n")
    print(indent + indent + "Calculation diff-diff for contacts...")
    df_contacts_control = pd.read_csv(os.path.join(input_path, "contacts_control.csv"),  parse_dates=["date"])
    df_contacts_treatment = pd.read_csv(os.path.join(input_path, "contacts_treatment.csv"),  parse_dates=["date"])
    df_contacts_treatment.set_index("date", inplace=True)
    df_contacts_control.set_index("date", inplace=True)
    for d in treatment_dates:
        treatment_date = d[0]
        end_date = d[1]
        window = f"({treatment_date.strftime('%Y-%m-%d')}, {end_date.strftime('%Y-%m-%d')})"
        treatment_0 = df_contacts_treatment.at[treatment_date, "percentage_change_trtm"]
        treatment_1 = df_contacts_treatment.at[end_date, "percentage_change_trtm"]
        control_0 = df_contacts_control.at[treatment_date, "percentage_change_ctrl"]
        control_1 = df_contacts_control.at[end_date, "percentage_change_ctrl"]
        diff-diff = (treatment_1 - control_1) - (treatment_0 - control_0)
        f.write(f"{window}\tcontacts\t{diff-diff}\n")