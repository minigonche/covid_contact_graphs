# Loads the different libraries

import numpy as np
import pandas as pd
import igraph as ig
import seaborn as sns
import geopandas as gpd
import contextily as ctx
from datetime import timedelta
import matplotlib.pyplot as plt
from google.cloud import bigquery
import os, sys


# Adds the general path
curr_path = os.path.dirname(os.path.realpath(__file__))
global_path = os.path.join(curr_path, '..','..')
sys.path.append(global_path)

from functions.utils import *
import figure_generation.constants as con
from figure_generation.edgelist_funcitons import *

# Starts the client
client = bigquery.Client(location="US")

# Constants
date_format = "%Y-%m-%d"
eps = 0.001
total_places = 10
percentage = 0.1
round_number = 4 # 15m

ident = '   '


def filter(df_geo, min_lat, max_lat, min_lon, max_lon):
    resp = df_geo[(df_geo.lat >= min_lat) & (df_geo.lat <= max_lat)].copy()
    resp = resp[(resp.lon >= min_lon) & (resp.lon <= max_lon)]
    return(resp)

# Main method
def main(report_name, locations_id, dataset_id, min_date, max_date, dif = None, gdf_polygon = None):
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

    if (dif is None) and (gdf_polygon is None):

        print(f'WARNING: No filtering method was given, it is possible that the resulting figure will be too zoomed out. \
        To get an appropriate plotting window provide either a number of standard deviations (through the parameter "dif") or \
            a geopandas with a polygon (through the parameter "gdf_polygon") to get the desired area in focus.')


    # export location
    export_folder_location = os.path.join(con.reports_folder_location, report_name, con.figure_folder_name)
    if not os.path.exists(export_folder_location):
        os.makedirs(export_folder_location)    
    
    location_pagerank = []
    location_count = []

    current_date = min_date

    print(ident + f'Computing Centralities from: {min_date.strftime(date_format)} to {max_date.strftime(date_format)}')

    while current_date <= max_date:
        
        current_date_string = current_date.strftime(date_format)
        
        print(ident + f'   Date: {current_date_string}')

            
        for hour in range(6,22):
            
            #print(f'   Hour:{hour}')
            
            edges = get_contacts(dataset_id, location_id, current_date_string, hour)        
            nodes = pd.concat((edges[['id1']].rename(columns = {'id1':'identifier'}), edges[['id2']].rename(columns = {'id2':'identifier'})), ignore_index = True).drop_duplicates()
            
            new_edges = sort_edges(nodes, edges, 'pagerank')
            
            # Extract location
            chunk = int(np.ceil(new_edges.shape[0]*percentage))
            df_temp = new_edges.iloc[0:chunk]

            # Selects lat and lon
            df_temp = df_temp[['lat','lon']].copy()
            df_temp['total'] = 1
            df_temp = df_temp.groupby(['lat','lon']).sum().reset_index()
            
            # Adds the timestamp
            df_temp['date'] = current_date
            df_temp['hour'] = hour              
            
            location_pagerank.append(df_temp)

        
        current_date = current_date + timedelta(days = 1)


    # Merges
    df_locations_pagerank = pd.concat(location_pagerank, ignore_index = True)

    if dif != None:
    
        #Removes outliers
        df_locations_pagerank = df_locations_pagerank[(np.abs(df_locations_pagerank.lon - df_locations_pagerank.lon.mean()) < dif*df_locations_pagerank.lon.std()) & (np.abs(df_locations_pagerank.lat - df_locations_pagerank.lat.mean()) < dif*df_locations_pagerank.lat.std())].copy()


    # NUM CONTACTS
    # ---------------------------
    # Calculate top places with more contacts
    df_locations = get_contacs_by_location(dataset_id, location_id, min_date.strftime(date_format), max_date.strftime(date_format))

    df_locations = df_locations.head(total_places)

    df1 = df_locations[['lat','lon','total_contacts']].rename(columns = {'total_contacts':'total'})

    # Adds noise
    df1.lat = df1.lat + np.random.normal(0,eps,df1.shape[0])
    df1.lon = df1.lon + np.random.normal(0,eps,df1.shape[0])


    geo_locations = gpd.GeoDataFrame(df1,crs =  "EPSG:4326", geometry=gpd.points_from_xy(df1.lon, df1.lat))
    geo_locations = geo_locations.to_crs(epsg=3857)

    # PAGERANK
    # Top Places
    # ---------------------------

    # Calculate top places more frequently selected when removing 10% of edges
    df2 = df_locations_pagerank[['lat','lon','total']].copy()
    df2.lon = df2.lon.round(round_number)
    df2.lat = df2.lat.round(round_number)

    df2 = df2.groupby(['lat','lon']).sum().reset_index().sort_values('total', ascending = False).head(total_places)

    df2.lat = df2.lat + np.random.normal(0,eps,df2.shape[0])
    df2.lon = df2.lon + np.random.normal(0,eps,df2.shape[0])

    geo_pagerank = gpd.GeoDataFrame(df2,crs =  "EPSG:4326", geometry=gpd.points_from_xy(df2.lon, df2.lat))
    geo_pagerank = geo_pagerank.to_crs(epsg=3857)


    # PAGERANK
    # Traces
    # ---------------------------

    # Calculate places 
    df3 = df_locations_pagerank[['lat','lon','total']].copy()
    df3 = df3.groupby(['lat','lon']).sum().reset_index()

    geo_pagerank_trace = gpd.GeoDataFrame(df3,crs =  "EPSG:4326", geometry=gpd.points_from_xy(df3.lon, df3.lat))
    geo_pagerank_trace = geo_pagerank_trace.to_crs(epsg=3857)

    if gdf_polygon != None:
        bounds = gdf_polygon.geometry.bounds
        min_lat = bound.miny.min
        min_lon = bound.minx.min
        max_lat = bound.maxy.max
        max_lon = bound.maxx.max

        geo_pagerank = filter(geo_pagerank, min_lat, max_lat, min_lon, max_lon)
        geo_pagerank_trace = filter(geo_pagerank_trace, min_lat, max_lat, min_lon, max_lon)
        geo_locations = filter(geo_locations, min_lat, max_lat, min_lon, max_lon)

        
    ax = geo_pagerank_trace.plot(figsize=(12, 12), alpha=0.1, markersize = 17, color = 'green', label = 'Pagrank (Traza)')
    geo_pagerank.plot(alpha=1, markersize = 35, color = 'blue', ax = ax, label = 'Pagrank (Top)')
    geo_locations.plot(alpha=1, markersize = 35, color = 'red', ax = ax, label = 'Contactos (Top)')
    ctx.add_basemap(ax, source=ctx.providers.OpenTopoMap)
    ax.set_axis_off()
    ax.set_title(f'Superdispersión ({min_date.strftime(date_format)} - {max_date.strftime(date_format)})')
    ax.legend()
    ax.figure.savefig(os.path.join(export_folder_location, 'edge_detection.png'), dpi = 150)
    
    
    df_pagerank = geo_pagerank.to_crs(epsg=4326)[['geometry']]
    df_pagerank['tipo'] = 'Pagerank Top'
    
    df_contactos = geo_locations.to_crs(epsg=4326)[['geometry']]
    df_contactos['tipo'] = 'Contactos Top'    

    df_pagerank_trace = geo_pagerank_trace.to_crs(epsg=4326)[['geometry']]
    df_pagerank_trace['tipo'] = 'Pagerank Traza'
    
    df_export = pd.concat((df_pagerank, df_contactos, df_pagerank_trace), ignore_index = False)
    df_export['lon'] = df_export.geometry.x
    df_export['lat'] = df_export.geometry.y
    
    df_export.to_csv(os.path.join(export_folder_location, 'edge_detection.csv'), index = False)
    
    
    
if __name__ == "__main__":

    # Reads the parameters from excecution
    report_name  = sys.argv[1] # report name
    location_id = sys.argv[2]
    num_days_back = int(sys.argv[3]) # num_days_back

    if len(sys.argv) > 4:
        dif = float(sys.argv[4]) # std-dev for filtering out outliers
    else: 
        dif = 2

    # If a polygon is given to delimit plotting area
    if len(sys.argv) > 5:

        polygon = sys.argv[5]
        gdf_polygon = gpd.read_file(polygon)
    else:
        gdf_polygon = None

    max_date =  pd.to_datetime(datetime.today()) # max date
    #max_date =  pd.to_datetime("2020-11-30")
    min_date = max_date - timedelta(days = num_days_back)

    weighted = False
    dataset_id = 'edgelists_cities'

    main(report_name, location_id, dataset_id, min_date, max_date, dif, gdf_polygon)

    
