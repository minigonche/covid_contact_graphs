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
import constants as con

# Starts the client
client = bigquery.Client(location="US")
job_config = bigquery.QueryJobConfig(allow_large_results = True)

# Constants
indent = con.indent
WINDOW_SIZE = 7 #days

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

def calculate_graph_attributes():
    return NotImplemented
    
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
    treatment_polygon = sys.argv[5]
    calculate_graph_attributes()
elif search_type == "polygon_name":
    control_polygon_name = sys.argv[4]
    sql_0 = f"""
        SELECT *
        FROM graph_attributes.graph_attributes
        WHERE location_id="{control_polygon_name}" 
            AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """
    treatment_polygon_name = sys.argv[5]
    sql_1 = f"""
        SELECT *
        FROM graph_attributes.graph_attributes
        WHERE location_id="{treatment_polygon_name}"  
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
print(indent + f"Retreiving control graph-attribites between {start_date} and {end_date}.")
query_job = client.query(sql_0, job_config=job_config) 

# Return the results as a pandas DataFrame
df_graph_attr_control = query_job.to_dataframe()

if df_graph_attr_control.empty:
    raise Exception("No graph-attrs found for the given control.")
    
df_graph_attr_control["date"] = pd.to_datetime(df_graph_attr_control["date"])

# Make query for treatment
print(indent + f"Retreiving treatment graph-attribites between {start_date} and {end_date}.")
query_job = client.query(sql_1, job_config=job_config) 

# Return the results as a pandas DataFrame
df_graph_attr_treatment = query_job.to_dataframe()

# df_graph_attr_treatment.to_csv(os.path.join(export_folder_location, "tmp", "df_graph_attr_treatment.csv"), index=False)
# df_graph_attr_control.to_csv(os.path.join(export_folder_location, "tmp", "df_graph_attr_control.csv"), index=False)

if df_graph_attr_treatment.empty:
    raise Exception("No graph-attrs found for the given treatment.")
    
df_graph_attr_treatment["date"] = pd.to_datetime(df_graph_attr_treatment["date"])

# Plotting
print(indent + "\tPlotting...")
df_graph_attr_control.drop(columns=["type"], inplace=True)
df_graph_attr_treatment.drop(columns=["type"], inplace=True)

df_graph_attr_control_plt = df_graph_attr_control.pivot_table(values='attribute_value', index='date', columns='attribute_name', aggfunc='first')
df_graph_attr_treatment_plt = df_graph_attr_treatment.pivot_table(values='attribute_value', index='date', columns='attribute_name', aggfunc='first')
df_graph_attr_control_plt.reset_index(inplace=True)
df_graph_attr_treatment_plt.reset_index(inplace=True)

attrs = ['graph_num_edges', 'eigenvector_gini_index', 'graph_transitivity', 'powerlaw_degree_ks_statistic', 'powerlaw_degree_alpha', 'pagerank_gini_index', 'graph_size', 'powerlaw_degree_p_value', 'powerlaw_degree_is_dist']

subplotsize=[10.,15.]
margin_left = 1.0
margin_right = 1.0
margin_top = 1.0
margin_bottom = 1.0
figuresize = [margin_left + subplotsize[0] + margin_right, margin_top + subplotsize[1] + margin_bottom]
left = 0.5*(2.-subplotsize[0]/figuresize[0])
right = 2.-left
bottom = 0.5*(1.-subplotsize[1]/figuresize[1])
top = 1.-bottom

fig, (ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9) = plt.subplots(9, figsize=(figuresize[0],figuresize[1]))
fig.subplots_adjust(left=left,right=right,bottom=bottom,top=top)
fig.suptitle('Graph Attributes')
fig.tight_layout()
axes = fig.get_axes()
for i in range(len(attrs)):
    ax = axes[i]
    attr = attrs[i]
    ax.plot(df_graph_attr_control_plt.date, df_graph_attr_control_plt[attr], label="control")
    ax.plot(df_graph_attr_treatment_plt.date, df_graph_attr_treatment_plt[attr], label="treatment")
    ax.axvline(x=treatment_date, label="Intervención", color="grey", linestyle='--')
    ax.set_ylabel(attr)
    
print(indent + "\tExporting...")
fig.savefig(os.path.join(export_folder_location, f"attrs-{control_polygon_name}-{treatment_polygon_name}.png"))

