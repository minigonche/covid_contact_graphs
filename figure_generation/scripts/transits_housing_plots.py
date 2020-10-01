# Script that generates the transits plots for the last month

# python figure_generation/scripts/transits_housing_plots.py reporte_norte_de_santander 30 colombia_cucuta


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


        
        

# Main method
def main(report_name, location_id, min_date, max_date):
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

    print(ident + 'Plots Trasits Housing')
    # export location
    export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
    
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)

    # Transforms the dates
    min_date_str = min_date.strftime(utils.date_format)
    max_date_str = max_date.strftime(utils.date_format)

    # Gets the nodes with housing location

    query = f"""

            SELECT h.identifier, 
                   h.lon, 
                   h.lat, 
                   h.type,
                   ST_DWithin(ST_GeogPoint(h.lon, h.lat),
                        (SELECT geometry 
                        FROM grafos-alcaldia-bogota.geo.locations_geometries 
                        WHERE location_id = "{location_id}"),
                        (SELECT precision 
                        FROM grafos-alcaldia-bogota.geo.locations_geometries 
                        WHERE location_id = "{location_id}")) as inside
            FROM 
            (SELECT identifier
            FROM grafos-alcaldia-bogota.transits.hourly_transits
            WHERE location_id = "{location_id}"
                  AND date >= "{min_date_str}" 
                  AND date <= "{max_date_str}"
                  

            GROUP BY identifier) as nodes
            JOIN 
            (SELECT *
            FROM housing_location.colombia_housing_location
            WHERE week_date >= "{min_date_str}" 
                    AND week_date <= "{max_date_str}") as h
            ON nodes.identifier = h.identifier

    """


    job_config = bigquery.QueryJobConfig(allow_large_results=True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()

    # Selects only the houses
    df = df[df['type'] == 'HOUSE'].copy()


    print(ident + "   National")

    # Converts to geopandas
    df_plot = df
    geo_df = geopandas.GeoDataFrame(df_plot, crs =  "EPSG:4326", geometry=geopandas.points_from_xy(df_plot.lon, df_plot.lat))
    geo_df = geo_df.to_crs(epsg=3857)


    ax = geo_df.plot(figsize=(8,8), alpha=0.5, markersize = 3, color = 'red')
    ctx.add_basemap(ax)
    ax.set_axis_off()
    ax.set_title('Casas de Dispositivos Transitorios (Nacional)')

    ax.figure.savefig(os.path.join(export_folder_location, f'trasits_housing_global.png'))
    # ------------------------------    

    print(ident + "   Local")

    df_plot = df[df.inside]
    geo_df = geopandas.GeoDataFrame(df_plot, crs =  "EPSG:4326", geometry=geopandas.points_from_xy(df_plot.lon, df_plot.lat))
    geo_df = geo_df.to_crs(epsg=3857)


    ax = geo_df.plot(figsize=(8,8), alpha=0.3, markersize = 0.5, color = 'red')
    ctx.add_basemap(ax)
    ax.set_axis_off()
    ax.set_title('Casas de Dispositivos Transitorios (Local)')

    ax.figure.savefig(os.path.join(export_folder_location, f'trasits_housing_locall.png'))
    # ------------------------------    


if __name__ == "__main__":

    # Reads the parameters from excecution
    report_name  = sys.argv[1] # report name
    num_days_back = int(sys.argv[2]) # num_days_back
    location_id =  sys.argv[3] # regchain
    
    max_date =  pd.to_datetime(datetime.today()) # max date
    min_date = max_date - timedelta(days = num_days_back)
    
    main(report_name, location_id, min_date, max_date)



    


