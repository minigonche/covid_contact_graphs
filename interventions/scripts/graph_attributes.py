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
control_flag = False

translate_title = {"eigenvector_gini_index": "Índice Gini de los vectores própios", 
            "graph_num_edges": "Número de aristas en el grafo",
            "graph_size": "Tamaño del grafo",
            "graph_transitivity": "Transitividad del grafo",
            "largest_eigenvalue_unweighted": "Mayor valor própio (sin ponderar)",
            "largest_eigenvalue_weighted": "Mayor valor própio (ponderado)",
            "number_of_cases_accumulated": "Número de casos acumulados",
            "number_of_contacts": "Número de contactos",
            "pagerank_gini_index":"Índice Gini del PageRank",
            "personalized_pagerank_gini_index":"Índice Gini del PageRank Personalizado",
            "powerlaw_degree_alpha": "",
            "powerlaw_degree_is_dist": "",
            "powerlaw_degree_ks_statistic": "",
            "powerlaw_degree_p_value": ""}



translate_ylabel = {"eigenvector_gini_index": "Índice Gini de los vectores própios", 
            "graph_num_edges": "Número de aristas",
            "graph_size": "Tamaño",
            "graph_transitivity": "Transitividad",
            "largest_eigenvalue_unweighted": "Valor própio",
            "largest_eigenvalue_weighted": "Valor própio",
            "number_of_cases_accumulated": "Número de casos",
            "number_of_contacts": "Número de contactos",
            "pagerank_gini_index":"Índice Gini",
            "personalized_pagerank_gini_index":"Índice Gini Personalizado",
            "powerlaw_degree_alpha": "",
            "powerlaw_degree_is_dist": "",
            "powerlaw_degree_ks_statistic": "",
            "powerlaw_degree_p_value": ""}


COLOR_CTRL = "#79c5b4"
COLOR_TRT = "#324592"
COLOR_HIHGLIGH = '#ff8c65'


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
treatment_polygon_name = sys.argv[3] # treatment_polygon
control_polygon_name = sys.argv[4] # control_polygon
treatment_date = sys.argv[5] # treatment_date
    
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
    FROM graph_attributes.graph_attributes
    WHERE location_id="{treatment_polygon_name}" 
        AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """

# Make query for treatment
print(indent + f"Retreiving treatment graph-attributes between {start_date} and {end_date}.")
query_job = client.query(sql_0, job_config=job_config) 

# Return the results as a pandas DataFrame
df_graph_attr_treatment = query_job.to_dataframe()

if df_graph_attr_treatment.empty:
    raise Exception("No graph-attrs found for the given treatment.")
    
df_graph_attr_treatment["date"] = pd.to_datetime(df_graph_attr_treatment["date"])

if control_polygon_name != "None":
    control_flag = True
    # export location
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}-{control_polygon_name}", "graph_attr")
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 
else:
    # export location
    export_folder_location = os.path.join(export_folder_location, f"{treatment_polygon_name}", "graph_attr")
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location) 

if control_flag:
    sql_1 = f"""
        SELECT *
        FROM graph_attributes.graph_attributes
        WHERE location_id="{control_polygon_name}"  
            AND date >= "{start_date.strftime("%Y-%m-%d")}" AND date <= "{end_date.strftime("%Y-%m-%d")}"
    """

    # Make query for treatment
    print(indent + f"Retreiving control graph-attribites between {start_date} and {end_date}.")
    query_job = client.query(sql_1, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df_graph_attr_control = query_job.to_dataframe()

    if df_graph_attr_control.empty:
        print("WARNING: No graph-attrs found for the given control. Running script only for treatment polygon")
        control_flag = False
    else:
        df_graph_attr_control["date"] = pd.to_datetime(df_graph_attr_control["date"])

        df_graph_attr_control.drop(columns=["type"], inplace=True)
        df_graph_attr_control_plt = df_graph_attr_control.pivot_table(values='attribute_value', index='date', columns='attribute_name', aggfunc='first')
        df_graph_attr_control_plt.reset_index(inplace=True)
        df_graph_attr_control_plt.sort_values(by="date", inplace=True)
    
# Plotting
print(indent + "\tPlotting...")
df_graph_attr_treatment.drop(columns=["type"], inplace=True)
df_graph_attr_treatment_plt = df_graph_attr_treatment.pivot_table(values='attribute_value', index='date', columns='attribute_name', aggfunc='first')
df_graph_attr_treatment_plt.reset_index(inplace=True)
df_graph_attr_treatment_plt.sort_values(by="date", inplace=True)

if control_flag:
    attrs = set(df_graph_attr_treatment_plt.columns).intersection(set(df_graph_attr_control_plt.columns))
    attrs = list(attrs - set(["date"]))
else:
    attrs = list(set(df_graph_attr_treatment_plt.columns) - set(["date"]))
    
print(indent + f"Plotting...")
for attr in attrs:    
    print(indent + f"\t{attr}")
    fig, ax = plt.subplots() 
    ax.set_title(translate_title[attr])
    ax.set_xlabel("Fecha")
    ax.set_ylabel(translate_ylabel[attr])
    maxy = df_graph_attr_treatment_plt[attr].max()
    miny = df_graph_attr_treatment_plt[attr].min()
    d = pd.date_range(treatment_date, end_date).values
    fig.set_figheight(5)
    fig.set_figwidth(15)
    ax.plot(df_graph_attr_treatment_plt["date"], df_graph_attr_treatment_plt[attr], linewidth=1, color=COLOR_TRT, label=f"{treatment_polygon_name} (tratamiento)")
    fig_name = f"{attr}.png"
    if control_flag:
        maxy = max(df_graph_attr_control_plt[attr].max(), df_graph_attr_treatment_plt[attr].max())
        miny = min(df_graph_attr_control_plt[attr].min(), df_graph_attr_treatment_plt[attr].min())
        ax.plot(df_graph_attr_control_plt["date"], df_graph_attr_control_plt[attr], linewidth=1, color=COLOR_CTRL, label=f"{control_polygon_name} (control)")
    ax.fill_between(d, maxy, miny, facecolor=COLOR_HIHGLIGH, alpha = 0.25) 
    ax.axvline(treatment_date, linestyle="--", color=COLOR_HIHGLIGH, linewidth=1)
    ax.legend()
    fig.savefig(os.path.join(export_folder_location,fig_name))
print(indent + "Exporting...")



# ['graph_num_edges', 'eigenvector_gini_index', 'graph_transitivity', 'powerlaw_degree_ks_statistic', 'powerlaw_degree_alpha', 'pagerank_gini_index', 'graph_size', 'powerlaw_degree_p_value', 'powerlaw_degree_is_dist']

# subplotsize=[10.,15.]
# margin_left = 1.0
# margin_right = 1.0
# margin_top = 1.0
# margin_bottom = 1.0
# figuresize = [margin_left + subplotsize[0] + margin_right, margin_top + subplotsize[1] + margin_bottom]
# left = 0.5*(2.-subplotsize[0]/figuresize[0])
# right = 2.-left
# bottom = 0.5*(1.-subplotsize[1]/figuresize[1])
# top = 1.-bottom

# fig, (ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9) = plt.subplots(9, figsize=(figuresize[0],figuresize[1]))
# fig.subplots_adjust(left=left,right=right,bottom=bottom,top=top)
# fig.suptitle('Graph Attributes')
# fig.tight_layout()
# axes = fig.get_axes()
# for i in range(len(attrs)):
#     ax = axes[i]
#     attr = attrs[i]
#     ax.plot(df_graph_attr_control_plt.date, df_graph_attr_control_plt[attr], label="control")
#     ax.plot(df_graph_attr_treatment_plt.date, df_graph_attr_treatment_plt[attr], label="treatment")
#     ax.axvline(x=treatment_date, label="Intervención", color="grey", linestyle='--')
#     ax.set_ylabel(attr)
    
# print(indent + "\tExporting...")
# fig.savefig(os.path.join(export_folder_location, f"attrs-{control_polygon_name}-{treatment_polygon_name}.png"))

