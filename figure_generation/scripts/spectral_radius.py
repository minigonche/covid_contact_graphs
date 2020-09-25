import numpy as np
import pandas as pd
import igraph as ig
import seaborn as sns
import geopandas as gpd
import contextily as ctx
from datetime import timedelta
import matplotlib.pyplot as plt
from google.cloud import bigquery

from functions.utils import get_edges, get_nodes

# Constants
date_format = "%Y-%m-%d"
pre_quarentine_start = pd.to_datetime("2020-03-05")
pre_quarentine_end = pd.to_datetime("2020-03-11")
quarentine_start = pd.to_datetime("2020-04-12")
quarentine_end = pd.to_datetime("2020-04-18")
pos_quarentine_start =  pd.to_datetime("2020-07-10")
pos_quarentine_end =  pd.to_datetime("2020-07-16")

def pre_quarentine(dataset_id, location_id):
    # ---------------------- Pre quarantine ----------------------- #

    print('Pre')
    print('   Extracting Edges')
    df_pre = get_edges(dataset_id, location_id, pre_quarentine_start.strftime(date_format), pre_quarentine_end.strftime(date_format))


    print('   Extracting Nodes')
    df_pre_nodes = pd.concat((df_pre[['id1']].drop_duplicates().rename(columns = {'id1':'identifier'}), df_pre[['id2']].drop_duplicates().rename(columns = {'id2':'identifier'})), ignore_index = True)
    df_pre_nodes = df_pre_nodes.drop_duplicates()

    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(df_pre_nodes.identifier.values)        
    G.add_edges(df_pre.apply(lambda df: (df.id1, df.id2), axis = 1))

    degrees = np.array(G.indegree()) + np.array(G.outdegree())
        
    print(f"   {pre_quarentine_start.strftime('%Y-%m-%d')} - {pre_quarentine_end.strftime('%Y-%m-%d')}.  Nodes: {df_pre_nodes.shape[0]} Edges: {df_pre.shape[0]} Min Degree: {np.min(degrees)} Max Degree: {np.max(degrees)} Mean Degree: {np.round(np.mean(degrees),2)}")
    print('')

    return [df_pre, df_pre_nodes]

def quarentine(dataset_id, location_id):  
    # ---------------------- quarantine ----------------------- #
    print('Quarantine')
    print('   Extracting Edges')
    df_quar = get_edges(dataset_id, location_id, quarentine_start.strftime(date_format), quarentine_end.strftime(date_format) )


    print('   Extracting Nodes')
    df_quar_nodes = pd.concat((df_quar[['id1']].drop_duplicates().rename(columns = {'id1':'identifier'}), df_quar[['id2']].drop_duplicates().rename(columns = {'id2':'identifier'})), ignore_index = True)
    df_quar_nodes = df_quar_nodes.drop_duplicates()

    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(df_quar_nodes.identifier.values)        
    G.add_edges(df_quar.apply(lambda df: (df.id1, df.id2), axis = 1))

    degrees = np.array(G.indegree()) + np.array(G.outdegree())
        
    print(f"   {quarentine_start.strftime('%Y-%m-%d')} - {quarentine_end.strftime('%Y-%m-%d')}.  Nodes: {df_quar_nodes.shape[0]} Edges: {df_quar.shape[0]} Min Degree: {np.min(degrees)} Max Degree: {np.max(degrees)} Mean Degree: {np.round(np.mean(degrees),2)}")
    print('')

    return [df_quar, df_quar_nodes]

def pos_quarentine(dataset_id, location_id):
    # ---------------------- Pos quarantine ----------------------- #

    print('Pos')
    print('   Extracting Edges')
    df_pos = get_edges(dataset_id, location_id, pos_quarentine_start.strftime(date_format), pos_quarentine_end.strftime(date_format) )


    print('   Extracting Nodes')
    df_pos_nodes = pd.concat((df_pos[['id1']].drop_duplicates().rename(columns = {'id1':'identifier'}), df_pos[['id2']].drop_duplicates().rename(columns = {'id2':'identifier'})), ignore_index = True)
    df_pos_nodes = df_pos_nodes.drop_duplicates()

    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(df_pos_nodes.identifier.values)        
    G.add_edges(df_pos.apply(lambda df: (df.id1, df.id2), axis = 1))



    degrees = np.array(G.indegree()) + np.array(G.outdegree())
        
    print(f"   {pos_quarentine_start.strftime('%Y-%m-%d')} - {pos_quarentine_end.strftime('%Y-%m-%d')}.  Nodes: {df_pos_nodes.shape[0]} Edges: {df_pos.shape[0]} Min Degree: {np.min(degrees)} Max Degree: {np.max(degrees)} Mean Degree: {np.round(np.mean(degrees),2)}")
    print('')

    return [df_pos, df_pos_nodes]

def plot_spectral_radius(df_results, path):
    plt.figure(figsize=(10,5))
    ax = sns.lineplot(data = df_results, x = 'percentage', y = 'change', hue = 'method')
    ax.set_ylabel('% Cambio Radio Espectral', fontsize=12)
    ax.set_xlabel('% de Aristas Removidas', fontsize=12)
    ax.legend().texts[0].set_text("Método")
    ax.figure.savefig(path)



# Main method
def main(report_name, locations_id, dataset_id, min_date, max_date, dif = 2):
    '''
    Method that plots the different plots for movement.

    Parameters
    -----------
    report_name : str
        Name of the report folder
    location_ids : string list
        List with the locations to include
    min_date : pd.datetime
        Mins date for the movmement
    max_date : pd.datetime
        Max date for the movement

    '''

    # export location
    export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)

    methods = ['random','eigenvector','pagerank']
    step = 0.05
    max_remove = 0.6

    percentages = np.arange(0,max_remove,step)
    results = []
    current_date = start_date
    days_step = 3
    days_window = 7
    while current_date <= end_date:
        
        start_date_string = (current_date - timedelta(days = days_window)).strftime(date_format)
        end_date_string = current_date.strftime(date_format)
        
        print(f'Computing from: {start_date_string} to {end_date_string}')
        
        nodes = get_nodes(location_id, start_date_string, end_date_string )
        edges = get_edges(dataset_id, location_id, start_date_string, end_date_string)
        
        print(f'   Nodes: {nodes.shape[0]}')
        print(f'   Edges: {edges.shape[0]}')
        
        initial_eigenvalue = compute_eigenvalue(nodes, edges, weighted)
        
        for method in methods:

            print(f'   Method: {method}')
            new_edges = sort_edges(nodes, edges, method)

            new_eigenvalues = []
            for per in percentages:
                #print(f'      Percentage: {per}')
                chunk = int(np.round(new_edges.shape[0]*per))
                e = compute_eigenvalue(nodes, new_edges.iloc[chunk:], weighted)
                results.append({'method':method, 'percentage':per, 'value':e, 'change':100*(e-initial_eigenvalue)/initial_eigenvalue})
        
        current_date = current_date + timedelta(days = days_step)
    print('Done')                        

    df_results = pd.DataFrame(results)
    plot_spectral_radius(df_results, os.path.join(export_folder_location, "spectral_radius.png"))