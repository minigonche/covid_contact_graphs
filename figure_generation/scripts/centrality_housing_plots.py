# Script that generates the centrality plots for the last month

# python figure_generation/scripts/centrality_housing_plots.py reporte_norte_de_santander colombia_cucuta pagerank_centrality 30


import sys
import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import seaborn as sns
from datetime import timedelta
from shapely import wkt
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler

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



def scale(values, min_val, max_val):
    scaler = MinMaxScaler(feature_range = (min_val,max_val))
    scaler.fit(values.reshape(-1, 1))
    return(scaler.transform(values.reshape(-1, 1))[:,0])
        

# Main method
def main(report_name, location_id, attribute_name, min_date, max_date, num_top = 5000, dif = 2, min_node_size = 4, max_node_size = 10):
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

    print(ident + f'Plots Attribute: {attribute_name} Housing')
    # export location
    export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)

    min_date_str = min_date.strftime(utils.date_format)
    max_date_str = max_date.strftime(utils.date_format)


    # Gets the nodes with housing location

    query = f"""

            SELECT h.identifier, 
                   nodes.attribute_value,
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
            (SELECT identifier, AVG(attribute_value) as attribute_value
            FROM grafos-alcaldia-bogota.graph_attributes.node_attributes
            WHERE location_id = "{location_id}"
                  AND attribute_name = "{attribute_name}"
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

    
    # Converts to geopandas
    df_plot = df[(df['type'] == 'HOUSE') & (df.inside)]
    df_plot = df_plot.sort_values('attribute_value', ascending = False).drop_duplicates(subset = 'identifier', keep = 'first')
    df_plot = df_plot.head(min(num_top, df.shape[0]))


    df_export = df_plot.copy()


    #Removes outliers
    df_plot = df_plot[ np.abs(df_plot.lon - df_plot.lon.mean()) < dif*df_plot.lon.std()].copy()
    df_plot = df_plot[ np.abs(df_plot.lat - df_plot.lat.mean()) < dif*df_plot.lat.std()].copy()


    geo_df = geopandas.GeoDataFrame(df_plot, crs =  "EPSG:4326", geometry=geopandas.points_from_xy(df_plot.lon, df_plot.lat))
    geo_df = geo_df.to_crs(epsg=3857)

    #source=ctx.providers.CartoDB.VoyagerNoLabels)
    
    # Constructs Squares
    lon_center = (geo_df.lon.max() - geo_df.lon.min())/2 + geo_df.lon.min()
    lat_center = (geo_df.lat.max() - geo_df.lat.min())/2 + geo_df.lat.min()
    rad = max(geo_df.lon.max() - geo_df.lon.min(), geo_df.lat.max() - geo_df.lat.min())/2

    left = min(geo_df.lon.min(), lon_center - rad)
    right = max(geo_df.lon.max(), lon_center + rad)

    bottom = min(geo_df.lat.min(), lat_center - rad)
    top = max(geo_df.lat.max(), lat_center + rad)
    
    margins = pd.DataFrame({'lon':[left,left,right,right], 'lat':[top,bottom,top,bottom]})
    margins = geopandas.GeoDataFrame(margins, crs =  "EPSG:4326", geometry=geopandas.points_from_xy(margins.lon, margins.lat))
    margins = margins.to_crs(epsg=3857)
    
    # Adjust size
    geo_df['markersize'] = scale(geo_df.attribute_value.values, min_node_size, max_node_size)    


    print(ident + '   Plotting')
    ax = geo_df.plot(figsize= fig_size, alpha= 0.5, markersize = 'markersize', color = 'red')
    margins.plot(alpha=0.0, markersize = 1, color = 'white', ax = ax)
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.VoyagerNoLabels)
    ax.set_axis_off()
    ax.set_title(f'Casas de Dispositivos Centrales')
    ax.figure.savefig(os.path.join(export_folder_location, f'housing_{attribute_name}.png'))
    # ------------------------------    

    print(ident + '   Exporting')

    df_export = df_export[['identifier','lon','lat','attribute_value','type']].sort_values('attribute_value', ascending = False)

    df_export.loc[df_export['type'] == 'HOUSE', 'type'] = 'Casa'
    df_export.loc[df_export['type'] == 'WORK', 'type'] = 'Trabajo'
    df_export.loc[df_export['type'] == 'COMMON', 'type'] = 'Lugar Habitual'

    df_export = df_export.rename(columns = {'identifier':'Identificador', 'attribute_value':'centralidad', 'type': 'Tipo de Ubicacion'})

    # in csv
    df_export.to_csv(os.path.join(export_folder_location, f'housing_{attribute_name}.csv'), index = False)
    
    # in shapefile
    df_export = geopandas.GeoDataFrame(
        df_export, geometry=geopandas.points_from_xy(df_export.lon, df_export.lat))
    
    # Sets CRS
    df_export.geometry = df_export.geometry.set_crs('EPSG:4326')
    
    df_export.columns = ['ID', 'lon', 'lat', 'centr', 'tipo', 'geometry']
    
    # Creates folder if does not exists
    shape_export = os.path.join(export_folder_location, f'housing_{attribute_name}_shape')
    if not os.path.exists(shape_export):
        os.makedirs(shape_export)
        
        
    
    df_export.to_file(os.path.join(shape_export, f'housing_{attribute_name}.shp'))

    





if __name__ == "__main__":

    # python figure_generation/scripts/centrality_housing_plots.py reporte_norte_de_santander colombia_cucuta pagerank_centrality 30
    
    # Reads the parameters from excecution
    report_name  = sys.argv[1] # report name
    location_id =  sys.argv[2] # location_id
    attribute_name = sys.argv[3] # attribute name
    num_days_back = int(sys.argv[4]) # num_days_back
    
    num_top = 5000
    dif = 2
    if len(sys.argv) > 5:
         num_top = int(sys.argv[5])
            
    if len(sys.argv) > 6:
        dif = float(sys.argv[6])
        
    max_date =  pd.to_datetime(datetime.today()) # max date
    min_date = max_date - timedelta(days = num_days_back)
    
    main(report_name, location_id, attribute_name, min_date, max_date, num_top = num_top, dif = dif)



    


