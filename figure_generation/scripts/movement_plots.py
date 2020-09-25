# Script that generates the movement plots for a selection of graphs

# python figure_generation/scripts/movement_plots.py reporte_norte_de_santander 30 colombia_cucuta_comuna_*


import sys
import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import seaborn as sns
from datetime import timedelta
from shapely import wkt
import matplotlib.pyplot as plt

import geopandas
import contextily as ctx

from datetime import datetime, timedelta
import re


# Adds the general path
curr_path = os.path.dirname(os.path.realpath(__file__))
global_path = os.path.join(curr_path, '..','..')
sys.path.append(global_path)

# imports utils
import functions.utils as utils

import figure_generation.constants as con



# Creates the client
client = bigquery.Client(location="US")

# Constants
ident = con.ident

# Figure size
fig_size = (6,6)


def label_for_publication(label):
    '''
    Edits the labels for publication
    '''

    # Cucuta
    if 'Cúcuta Comuna' in label:
        if 'Cúcuta Comuna 0' == label:
            return('')
        
        return(label.replace('Cúcuta ',''))
    
    if 'Palmira Comuna' in label:     
        return(label.replace('Palmira ',''))
    
    return(label)
        
        
        
def make_plot(polygons, df_mov, min_date, max_date, col, title, cmap = 'OrRd', labels = True, fig_size = fig_size):
    '''
    Method that makes plot
    '''

    df_sel = df_mov.loc[(df_mov.date >= min_date) & (df_mov.date <= max_date),['location_id',col]].groupby('location_id').mean().reset_index()

    df_plot = polygons.merge(df_sel, on = 'location_id')

    
    if 'percenta' in col:
        df_plot[col] = df_plot[col]*100

    ax = df_plot.plot(column = col, legend = True, cmap=cmap, figsize=fig_size)
    ax.set_title(title, fontsize = 16)
    ax.set_axis_off()
    if labels:
        df_plot.apply(lambda p: ax.text(s= label_for_publication(p['label']), 
                                        x=p.geometry.centroid.coords[0][0], 
                                        y = p.geometry.centroid.coords[0][1], 
                                        ha='center'),axis=1)
    

    return(ax)


# Main method
def main(report_name, locations_ids, min_date, max_date):
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


    # Loads the polygons
    df = utils.get_current_locations_complete(client, only_active = False)
    df['geometry'] = df['geometry'].apply(wkt.loads)
    polygons = geopandas.GeoDataFrame(df, geometry = 'geometry')
    polygons['label'] = polygons['name']
    polygons.index = polygons.location_id.values

    # Filters the polygons
    polygons = polygons[polygons.location_id.isin(locations_ids)].copy()


    # Extracts the movement
    sql = """
        SELECT *
        FROM graph_attributes.graph_movement
    """

    df_mov = utils.run_simple_query(client, sql)

    # Filters out
    df_mov = df_mov[df_mov.location_id.isin(polygons.location_id)].copy()

    #Starts the figure generation
    print(ident + f"Creating Movement plots for: {report_name} from {min_date} to {max_date}")

    print(ident + "   All Movement")
    # All Movement
    col = 'all_movement'
    title = 'Movimiento Total (KM)'
    ax = make_plot(polygons, df_mov, min_date, max_date, col, title, 'Reds')
    ax.figure.savefig(os.path.join(export_folder_location, f'network_{col}.png'))
    # ------------------------------

    print(ident + "   Inner Average Movment")
    # Inner Average
    col = 'inner_movement_avg'
    title = 'Movimiento Interno Promedio (KM)'
    ax = make_plot(polygons, df_mov, min_date, max_date, col, title, 'Purples')
    ax.figure.savefig(os.path.join(export_folder_location, f'network_{col}.png'))
    # ------------------------------    


    print(ident + "   Outer Average Movment")
    # Outer 
    col = 'outer_movement_avg'
    title = 'Movimiento Externo Promedio (KM)'
    ax = make_plot(polygons, df_mov, min_date, max_date, col, title, 'Blues')
    ax.figure.savefig(os.path.join(export_folder_location, f'network_{col}.png'))
    # ------------------------------    


    print(ident + "   Percentage Outside")
    # Percentage Outer 
    col = 'percentaga_treveled_outside'
    title = 'Porcentaje Que Sale del Territorio'
    ax = make_plot(polygons, df_mov, min_date, max_date, col, title, 'Oranges')
    ax.figure.savefig(os.path.join(export_folder_location, f'network_{col}.png'))
    # ------------------------------    


    print(ident + "   Percentage Inside")
    # Percentage Inside 
    col = 'percentaga_stayed_in'
    title = 'Porcentaje Que No Sale del Territorio'
    ax = make_plot(polygons, df_mov, min_date, max_date, col, title, 'Greens')
    ax.figure.savefig(os.path.join(export_folder_location, f'network_{col}.png'))
    # ------------------------------    

   

if __name__ == "__main__":

    # Reads the parameters from excecution
    report_name  = sys.argv[1] # report name
    num_days_back = int(sys.argv[2]) # num_days_back
    reg_chain =  sys.argv[3] # regchain
    
    max_date =  pd.to_datetime(datetime.today()) # max date
    min_date = max_date - timedelta(days = num_days_back)
    

    all_locations = utils.get_current_locations(client, only_active = False).location_id.values.tolist()

    regex = re.compile(reg_chain)
    locations_ids = list(filter(regex.search, all_locations))

    if len(locations_ids) == 0:
        raise ValueError('No locations ids found for given regex chain')

    main(report_name, locations_ids, min_date, max_date)



    


