# Loads the different libraries
import os
import sys
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import zscore
import matplotlib.pyplot as plt

# Gets config
import config_constants as con
import constants as const

# Constants
DIFFDIFF_CODES = const.diffdiff_codes
indent = const.indent
STEP = 5
COLOR_HIHGLIGH = '#ff8c65'
COLORS = ["#75D1BA", "#B0813B", "#A85238", "#18493F", "#32955E", 
          "#1C4054", "#265873", "#CF916E", "#DEAA9C", "#6B2624",
          "#BB3E94", "#2F8E31", "#CED175", "#85D686", "#A39FDF"]
NAN_FRAC = 0.5

TRANSLATE = {"contacts":"Contactos",
            "graph_attributes":"Atributos de grafos",
            "movement":"Movimiento"}

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

if len(sys.argv) > 3:
    end_date = sys.argv[3]
    end_date = pd.Timestamp(datetime.datetime.strptime(end_date, '%Y-%m-%d'))
else:
    end_date = pd.Timestamp(datetime.datetime.now())
    
# Convert dates to date object
treatment_date = pd.Timestamp(datetime.datetime.strptime(treatment_date, '%Y-%m-%d'))

steps = pd.date_range(treatment_date, end_date, periods=STEP).tolist()

# if delta > 30:
#     step = 10
# elif delta > 20:
#     step = 5
# else: step = 2 
   
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
        size = (df_tmp.shape[0] * df_tmp.shape[1])
        if df_tmp.isnull().sum().sum() / size >= NAN_FRAC:
            print(f"WARNING: Dataset is missing more than half of its values.")
        
        df_tmp = df_tmp[(df_tmp.index >= treatment_date) & (df_tmp.index <= end_date)]
        df_tmp.sort_index(inplace=True)

        try:
            baseline = (df_tmp.at[treatment_date, ctrl_column]) - (df_tmp.at[treatment_date, trtm_column])
        except KeyError:
            min_date = df_tmp.index.min()
            baseline = (df_tmp.at[min_date, ctrl_column]) - (df_tmp.at[min_date, trtm_column])            
            print(f"WARNING: treatment date {treatment_date} not found at index. Next available date {min_date}")
        
        if np.isnan(baseline):
            print("""WARNING: Can't establish baseline for diff-diff please check integrity of data. 
            Dafatrame might have too many missing values. Continuing with next variable""")
            continue    
        points = []
        dates = []
        for d in df_tmp.index[::STEP]:
            point = (df_tmp.at[d, ctrl_column]) - (df_tmp.at[d, trtm_column])
            points.append(point)
            dates.append(d)
        try:
            final = (df_tmp.at[end_date, ctrl_column]) - (df_tmp.at[end_date, trtm_column])
        except:
            max_date = df_tmp.index.max()
            final = (df_tmp.at[max_date, ctrl_column]) - (df_tmp.at[max_date, trtm_column])
        if final != points[-1]:
            points.append(final)
            dates.append(end_date)
            
        # Calculate diff-diff for each point   
        points = [(p / baseline) for p in points]
        df_diffdiff_tmp = pd.DataFrame({"date":dates, attr:points})
        df_diffdiff = df_diffdiff.merge(df_diffdiff_tmp, on="date", how="outer")

    # Drop rows without diff-diff
    df_diffdiff.set_index("date", inplace=True)
    df_diffdiff.dropna(inplace=True, how="all")
    print(indent + f"Plotting...")
    export_folder_location = os.path.join(folder_name, k)
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)
        
    for i in range(len(df_diffdiff.columns)):
        attr = df_diffdiff.columns[i]
        color = COLORS[i]
        ymin = df_diffdiff[attr].min()
        ymax = df_diffdiff[attr].max()
        plt.plot(df_diffdiff.index, df_diffdiff[attr], color=color, label=attr)
        plt.vlines(treatment_date, ymin, ymax, color=COLOR_HIHGLIGH, linestyle="--")
        plt.annotate("Tratamiento", (treatment_date,(ymax + ymin)/2), rotation=90, color=COLOR_HIHGLIGH)
        plt.title(f"{attr}")
        plt.xlabel("Fecha")
        plt.ylabel("diff-diff")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.savefig(os.path.join(export_folder_location, f"diff-diff_{attr}.png"))
        plt.close()
        
    df_diffdiff.to_csv(os.path.join(export_folder_location, "diff-diff.csv"))
    