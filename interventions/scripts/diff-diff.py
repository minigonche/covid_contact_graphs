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
# import config_constants as con
import constants as const

# Starts the client
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)

# Constants
DIFFDIFF_CODES = const.diffdiff_codes
indent = const.indent
COLOR_CTRL = const.COLOR_CTRL
COLOR_TRT = const.COLOR_TRT
COLOR_HIHGLIGH = const.COLOR_HIHGLIGH
WHIS = const.WHIS
COLORS = const.PALETTE
NAN_FRAC = 0.5

# Get translation table
sql = f"""
    SELECT *
    FROM graph_attributes.attribute_names 
    """

query_job = client.query(sql, job_config=job_config) 
df_translations = query_job.to_dataframe()

df_translations.set_index(["type", "attribute_name"], inplace=True)
print(df_translations.head())
def translate(attr_name, typ):
    translation = df_translations.iloc[(df_translations.index.get_level_values('type') == typ) \
                                       & (df_translations.index.get_level_values('attribute_name') == attr_name)]["translated_name"]

    return translation.values[0]

def get_yaxis_name(attr_name, typ):
    yaxis_name = df_translations.iloc[(df_translations.index.get_level_values('type') == typ) \
                                       & (df_translations.index.get_level_values('attribute_name') == attr_name)]["yaxis_name"]
    return yaxis_name.values[0]



def get_attrs(columns):
    attrs_codes = {}
    codes = set()
    for col in columns:
        col = col.strip()
        col = col.split("-")
        codes.add(col[0])
        attr_code = col[1]
        attr = col[2]
        typ = col[3]
        attrs_codes[attr_code] = attr
    if len(codes) != 1:
        raise Exception(f"WARNING: Check integrity of data, variables with different codes found. Codes: {codes}")
    return attrs_codes

# get input
folder_name = sys.argv[1]
treatment_date = sys.argv[2]
treatment_length = sys.argv[3]
end_date = sys.argv[4]
    
# Convert dates to date object
treatment_date = pd.Timestamp(datetime.datetime.strptime(treatment_date, '%Y-%m-%d'))
treatment_end = treatment_date + datetime.timedelta(days = treatment_length)
end_date = pd.Timestamp(datetime.datetime.strptime(end_date, '%Y-%m-%d'))

 
   
for k in DIFFDIFF_CODES.keys():
    print(indent + f"Calculating diff-diff for {k} variables.")
    file_name = f"{k}.csv"
    df = pd.read_csv(os.path.join(folder_name, file_name), parse_dates=["date"])
    df.set_index("date", inplace=True)
    code = DIFFDIFF_CODES[k]
    attrs_codes = get_attrs(df.columns)
    df_diffdiff = pd.DataFrame({"date":pd.date_range(start=treatment_date, end=end_date)})
    
    for attr_code in attrs_codes.keys():
        attr = attrs_codes[attr_code]
        print(indent + f"  {attr}.")
        ctrl_column = f"{code}-{attr_code}-{attr}-ctrl"
        trtm_column = f"{code}-{attr_code}-{attr}-trtm"
        df_tmp = df[[ctrl_column, trtm_column]].copy()
        
        # Check dataset integrity
        if df_tmp[trtm_column].isnull().sum() / len(df_tmp[trtm_column]) >= NAN_FRAC:
            print("WARNING: Treatment dataset is missing more than half of its values. Continuing with next variable.")
            continue
        if df_tmp[ctrl_column].isnull().sum() / len(df_tmp[ctrl_column]) >= NAN_FRAC:
            print("WARNING: Control dataset is missing more than half of its values. Continuing with next variable.")
            continue
        
        df_baseline = df_tmp[df_tmp.index < treatment_date].copy()
        baseline = df_baseline[ctrl_column].subtract(df_baseline[trtm_column]).mean()
        df_tmp = df_tmp[(df_tmp.index >= treatment_date) & (df_tmp.index <= end_date)]
        df_tmp.sort_index(inplace=True)
        
        # Calculate diff-diff
        df_tmp[f"{k}-{attr}"] = df_tmp[ctrl_column].subtract(df_tmp[trtm_column]).divide(baseline).multiply(100)
        df_diffdiff = df_diffdiff.merge(df_tmp[f"{k}-{attr}"], left_on="date", right_index=True, how="outer")
        

    if df_diffdiff.shape[1] <= 1:
        continue
        
    # Drop rows without diff-diff
    print(indent + f"Plotting...")
    df_diffdiff.set_index("date", inplace=True)
    df_diffdiff.dropna(inplace=True, how="all")
    export_folder_location = os.path.join(folder_name, k)
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)
    
    d = pd.date_range(treatment_date, treatment_end).values
    # Plotting diff-diff percentage change
    for i in range(len(df_diffdiff.columns)):
        column_name = df_diffdiff.columns[i]
        typ, attr = column_name.split("-")
        color = COLORS[i]
        plt.plot(df_diffdiff.index, df_diffdiff[column_name], color=color, label=f"{translate(attr, typ)}")
        ymin = df_diffdiff[column_name].min()
        ymax = df_diffdiff[column_name].max()
        plt.fill_between(d, ymax, ymin, facecolor=COLOR_HIHGLIGH, alpha = 0.25)
        plt.vlines(treatment_date, ymin, ymax, color=COLOR_HIHGLIGH, linestyle="--")
        plt.annotate("Tratamiento", (treatment_date,(ymax + ymin)/2), rotation=90, color=COLOR_HIHGLIGH)
        plt.title("Cambio porcentual en diff-diff")
        plt.xlabel("Fecha")
        plt.ylabel("Porciento (%)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.savefig(os.path.join(export_folder_location, f"diff-diff_{attr}.png"))
        plt.close()
        
    # Plotting diff-diff boxplot
    df_diffdiff.to_csv(os.path.join(export_folder_location, "diff-diff.csv"))
    