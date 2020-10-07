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
WINDOW_SIZE = const.WINDOW_SIZE
COLOR_CTRL = const.COLOR_CTRL
COLOR_TRT = const.COLOR_TRT
COLOR_HIHGLIGH = const.COLOR_HIHGLIGH
WHIS = const.WHIS

control_flag = False

ATTR_NAME = "graph_movement"
DIFFDIFF_CODE = const.diffdiff_codes[ATTR_NAME]


# Get translation table
sql = f"""
    SELECT *
    FROM graph_attributes.attribute_names 
    """

query_job = client.query(sql, job_config=job_config) 
df_translations = query_job.to_dataframe()

df_translations.set_index(["type", "attribute_name"], inplace=True)

def translate(attr_name):
    translation = df_translations.iloc[(df_translations.index.get_level_values('type') == ATTR_NAME) \
                                       & (df_translations.index.get_level_values('attribute_name') == attr_name)]["translated_name"]
    if len(translation) == 0:
        raise Exception(f"No translation found for attribute {attr_name} of type {ATTR_NAME}.\nPlease update graph_attributes.attribute_names table.")
    return translation.values[0]

def get_yaxis_name(attr_name):
    yaxis_name = df_translations.iloc[(df_translations.index.get_level_values('type') == ATTR_NAME) \
                                       & (df_translations.index.get_level_values('attribute_name') == attr_name)]["yaxis_name"]
    if len(yaxis_name) == 0:
        raise Exception(f"No translation found for attribute {attr_name} of type {ATTR_NAME}.\nPlease update graph_attributes.attribute_names table.")
    return yaxis_name.values[0]

if len(sys.argv) < 8:
    print(indent + "This scripts runs with the following args:")
    print(indent + "\t1. location_id*")
    print(indent + "\t2. report_name*")
    print(indent + "\t3. treatment polygon*")
    print(indent + "\t4. control polygon*")
    print(indent + "\t5. treatment_date*")
    print(indent + "\t6. treatment_date*")
    print(indent + "\t7. start_date*")
    print(indent + f"\t8. end_date*")
    raise Exception("Insufficient arguments. Please run again.")

# Reads the parameters from excecution
location_id  =  sys.argv[1] # location id
report_name  =  sys.argv[2] # report name
treatment_polygon_name = sys.argv[3] # treatment_polygon
control_polygon_name = sys.argv[4] # control_polygon
treatment_date = sys.argv[5] # treatment_date
treatment_length = sys.argv[6] # treatment_length
start_date = sys.argv[7] # Start_date
end_date = sys.argv[8] # End_date
    

# export location
export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)    
    

# Convert dates to datetime
treatment_date = pd.Timestamp(datetime.datetime.strptime(treatment_date, '%Y-%m-%d'))
treatment_end = treatment_date + datetime.timedelta(days = int(treatment_length))
start_date = pd.Timestamp(datetime.datetime.strptime(start_date, '%Y-%m-%d'))
end_date = pd.Timestamp(datetime.datetime.strptime(end_date, '%Y-%m-%d'))

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
    FROM graph_attributes.{ATTR_NAME}
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
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}-{control_polygon_name}", ATTR_NAME)
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)
    if not os.path.exists(export_folder_location_diffdiff):
        os.makedirs(export_folder_location_diffdiff)
    
else:
    # export location
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}", ATTR_NAME)
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 

attrs = list(set(df_mov_treatment.columns) - set(["location_id", "date"]))

if control_flag:
    sql_1 = f"""
        SELECT *
        FROM graph_attributes.{ATTR_NAME}
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
        df_diff_diff.to_csv(os.path.join(export_folder_location_diffdiff, f"{ATTR_NAME}.csv"), index=False)
        
    


print(indent + f"Plotting...")
# Plotting boxplots
if control_flag:
    df_mov_treatment_boxplot_pre = df_mov_treatment[df_mov_treatment["date"] < treatment_date].copy()
    df_mov_control_boxplot_pre = df_mov_control[df_mov_control["date"] < treatment_date].copy()  

    df_mov_treatment_boxplot_post = df_mov_treatment.loc[(df_mov_treatment["date"] >= treatment_date)\
                                                    & (df_mov_treatment["date"] <= treatment_end)].copy()
    df_mov_control_boxplot_post = df_mov_control.loc[(df_mov_control["date"] >= treatment_date)\
                                                    & (df_mov_control["date"] <= treatment_end)].copy()     
    
    for attr in attrs: 
        print(indent + f"\t{attr}")
        data_to_plot = [df_mov_treatment_boxplot_pre[attr], df_mov_control_boxplot_pre[attr], \
                        df_mov_treatment_boxplot_post[attr], df_mov_control_boxplot_post[attr]]
            
        # Create a figure instance
        fig, ax = plt.subplots(figsize=(10, 10))
        fig.subplots_adjust(bottom=0.5)   

        # Create the boxplot
        bp = ax.boxplot(data_to_plot, patch_artist=True, whis=WHIS)
        
        ## change outline color, fill color and linewidth of the boxes
        box_trtm_pre = bp['boxes'][0]
        box_ctrl_pre = bp['boxes'][1]
        box_trtm_pos = bp['boxes'][2]
        box_ctrl_pos = bp['boxes'][3]
        # change outline and fill color
        box_trtm_pre.set(color=COLOR_TRT, linewidth=2)
        box_ctrl_pre.set(color=COLOR_CTRL, linewidth=2)
        box_trtm_pos.set(color=COLOR_TRT, linewidth=2)
        box_ctrl_pos.set(color=COLOR_CTRL, linewidth=2)
        
        ## change color and linewidth of the whiskers
        whisker_trtm_pre_1 = bp['whiskers'][0]
        whisker_trtm_pre_2 = bp['whiskers'][1]
        whisker_ctrl_pre_1 = bp['whiskers'][2]
        whisker_ctrl_pre_2 = bp['whiskers'][3]
        whisker_trtm_pos_1 = bp['whiskers'][4]
        whisker_trtm_pos_2 = bp['whiskers'][5]
        whisker_ctrl_pos_1 = bp['whiskers'][6]
        whisker_ctrl_pos_2 = bp['whiskers'][7]
        
        
        whisker_trtm_pre_1.set(color=COLOR_TRT, linewidth=2)
        whisker_trtm_pre_2.set(color=COLOR_TRT, linewidth=2)
        whisker_ctrl_pre_1.set(color=COLOR_CTRL, linewidth=2)
        whisker_ctrl_pre_2.set(color=COLOR_CTRL, linewidth=2)
        whisker_trtm_pos_1.set(color=COLOR_TRT, linewidth=2)
        whisker_trtm_pos_2.set(color=COLOR_TRT, linewidth=2)
        whisker_ctrl_pos_1.set(color=COLOR_CTRL, linewidth=2)
        whisker_ctrl_pos_2.set(color=COLOR_CTRL, linewidth=2)

        ## change color and linewidth of the caps
        cap_trtm_pre_1 = bp['caps'][0]
        cap_trtm_pre_2 = bp['caps'][1]
        cap_ctrl_pre_1 = bp['caps'][2]
        cap_ctrl_pre_2 = bp['caps'][3]
        cap_trtm_pos_1 = bp['caps'][4]
        cap_trtm_pos_2 = bp['caps'][5]
        cap_ctrl_pos_1 = bp['caps'][6]
        cap_ctrl_pos_2 = bp['caps'][7]

        cap_trtm_pre_1.set(color=COLOR_TRT, linewidth=2)
        cap_trtm_pre_2.set(color=COLOR_TRT, linewidth=2)
        cap_ctrl_pre_1.set(color=COLOR_CTRL, linewidth=2)
        cap_ctrl_pre_2.set(color=COLOR_CTRL, linewidth=2)
        cap_trtm_pos_1.set(color=COLOR_TRT, linewidth=2)
        cap_trtm_pos_2.set(color=COLOR_TRT, linewidth=2)
        cap_ctrl_pos_1.set(color=COLOR_CTRL, linewidth=2)
        cap_ctrl_pos_2.set(color=COLOR_CTRL, linewidth=2)
        
        ## change color and linewidth of the medians        
        for median in bp['medians']:
            median.set(color=COLOR_HIHGLIGH, linewidth=2)

        ## change the style of fliers and their fill
        flier_trtm_pre = bp['fliers'][0]
        flier_ctrl_pre = bp['fliers'][1]
        flier_trtm_pos = bp['fliers'][2]
        flier_ctrl_pos = bp['fliers'][3]
        # change outline and fill color
        flier_trtm_pre.set(marker='o', markeredgecolor=COLOR_TRT, alpha=0.5, linewidth=2)
        flier_ctrl_pre.set(marker='o', markeredgecolor=COLOR_CTRL, alpha=0.5, linewidth=2)
        flier_trtm_pos.set(marker='o', markeredgecolor=COLOR_TRT, alpha=0.5, linewidth=2)
        flier_ctrl_pos.set(marker='o', markeredgecolor=COLOR_CTRL, alpha=0.5, linewidth=2)
            
        ##legend
        ax.legend([bp["boxes"][0], bp["boxes"][1]], ['Tratamiento', 'Control'], loc='upper left')
        
        ## Custom x-axis labels
        ax.set_xticklabels([f"{ge.wrap_text(data_name_trtm)}", f"{ge.wrap_text(data_name_ctrl)}", \
                            f"{ge.wrap_text(data_name_trtm)}", f"{ge.wrap_text(data_name_ctrl)}"])

        ax.annotate("Tratamiento", (0.5,0.5), xycoords='axes fraction' ,rotation=90, color=COLOR_HIHGLIGH)
        ax.axvline(2.5, linestyle="--", color=COLOR_HIHGLIGH, linewidth=1)
        ax.set_title(translate(attr))
        ax.set_ylabel(get_yaxis_name(attr))
        ## Remove top axes and right axes ticks
        ax.get_xaxis().tick_bottom()
        ax.get_yaxis().tick_left()
        
        fig.savefig(os.path.join(export_folder_location,f"{attr}_boxplot.png"))
        

# Plotting linegraphs
for attr in attrs:    
    print(indent + f"\t{attr}")
    fig, ax = plt.subplots() 
    ax.set_title(translate(attr))
    ax.set_xlabel("Fecha")
    ax.set_ylabel(get_yaxis_name(attr))
    maxy = df_mov_treatment[attr].max()
    miny = df_mov_treatment[attr].min()
    d = pd.date_range(treatment_date, treatment_end).values
    fig.set_figheight(5)
    fig.set_figwidth(15)
    ax.plot(df_mov_treatment["date"], df_mov_treatment[attr], linewidth=1, color=COLOR_TRT, label=f"{data_name_trtm} (pre-tratamiento) (tratamiento)")
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

    



