# from google.cloud import bigquery
# client = bigquery.Client()
# dataset_ref = client.dataset('my_dataset')

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from google.cloud.exceptions import NotFound
import pandas as pd
import time
import numpy as np
from datetime import timedelta, datetime
import os





# Global min date
global_min_date = pd.to_datetime('2021-01-01 00:00:00')
global_min_sunday = pd.to_datetime('2021-01-10 00:00:00')
global_min_housing_sunday = pd.to_datetime('2020-01-12 00:00:00')
global_min_seniority_search = pd.to_datetime("2020-11-01 00:00:00")
global_min_attribute_date = pd.to_datetime('2021-01-10 00:00:00')

# GLobal date fomat
date_format = '%Y-%m-%d'
long_date_format = '%Y-%m-%d %H:%M:%S'

# Global temp dataset
temp_data_set_id = "download_temp"

graphs_attribute_table = 'grafos-alcaldia-bogota.graph_attributes.graph_attributes'
nodes_attribute_table = 'grafos-alcaldia-bogota.graph_attributes.node_attributes'

bogota_codes = ['CO.34','CO.33']

global_accuracy = 30

# Days to lag the computation of graphs and attributes
# Unacast records do some backfilling
global_day_shift = 4

global_attribute_window = 7
global_attribute_window_shift_days = 3

# CITY codes
BOGOTA = "bogota"
    

debug = False
debug_current_date = "2021-01-01"




def get_today(only_date = False):
    '''
    Returns today (with shift for us time)
    '''
    
    if only_date:
        d = pd.to_datetime((datetime.now() - timedelta(days = global_day_shift, hours = 6)).strftime('%Y-%m-%d'))
    else:
        d = pd.to_datetime((datetime.now() - timedelta(days = global_day_shift, hours = 6)).strftime('%Y-%m-%d 00:00:00'))
    

    if debug:
        print()
        print('-----------------')
        print('-- DEBUG IS ON --')
        print('-----------------')
        print()

        if only_date:
            d = pd.to_datetime(debug_current_date)
        else:
            d = pd.to_datetime(debug_current_date + " 00:00:00")

    return(d)

def get_year_and_week_of_date(d):
    '''
    Returns the year and week of the year of the given date.
    First week is 1
    
    d is pandas.datetime
    
    will raise an error if the given date is not a sunday
    '''
    d = pd.to_datetime(d)
    
    if d.dayofweek != 6:
        raise ValueError('Extraction of week is only supported for sundays. But received: {}'.format(d))
    
    
    return(d.date().year,  d.date().isocalendar()[1])



def get_date_of_week(year, week):
    '''
    Returns the year and week of the year of the given date.
    
    Assumes first week is 1
    
    d is pandas.datetime
    '''
    
    if week <= 0:
        raise ValueError("Min week is 1, but {} received".format(week))
    if week > 53:
        raise ValueError("Min week is 53, but {} received".format(week))
        
    d = f"{year}-W{week -1}"
    r = pd.to_datetime(datetime.strptime(d + '-0', "%Y-W%W-%w"))
    
    return(r)


def run_simple_query(client, query, allow_large_results=False):
    '''
    Method that runs a simple query
    '''
    
    job_config = bigquery.QueryJobConfig(allow_large_results = allow_large_results)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    return(df)
    

    
def get_current_locations(client, only_active = True):
    '''
    Gets all the current locations
    '''
    
    if only_active:
        sql = f"""
            SELECT location_id, name, precision, dataset, type
            FROM grafos-alcaldia-bogota.geo.locations_geometries
            WHERE active = TRUE
            GROUP BY location_id, name, precision, dataset, type
      """

    else:
        sql = f"""
            SELECT location_id, name, precision, dataset, type
            FROM grafos-alcaldia-bogota.geo.locations_geometries
            GROUP BY location_id, name, precision, dataset, type
      """

    return( run_simple_query(client, sql))


def get_current_locations_complete(client, only_active = True):
    '''
    Gets all the current locations (including geometry)
    '''
    
    if only_active:
        sql = f"""
            SELECT *
            FROM grafos-alcaldia-bogota.geo.locations_geometries
            WHERE active = TRUE
            
      """

    else:
        sql = f"""
            SELECT *
            FROM grafos-alcaldia-bogota.geo.locations_geometries
      """

    return( run_simple_query(client, sql))


def get_current_locations_for_attributes(client):
    '''
    Gets all the current locations
    '''
    

    sql = f"""
        SELECT location_id, name, precision, dataset, type
        FROM grafos-alcaldia-bogota.geo.locations_geometries
        WHERE attribute_active = TRUE
        GROUP BY location_id, name, precision, dataset, type
      """

    return( run_simple_query(client, sql))


# Is in different locations ids
# ----------------

def get_city(client, location_id, df_codes = None):
    '''
    Gets the city
    '''
    
    if(is_in_bogota(client, location_id, df_codes = df_codes)):
        return BOGOTA
    
    if('colombia' in location_id):
        city = location_id.replace('colombia_','')
        if "_" in city:
            city = city.split('_')[0]
            
        return(city)
    elif('peru' in location_id):
        city = location_id.replace('peru_','')[0]
        if "_" in city:
            city = city.split('_')[0]
        return(city)
    else:
        raise ValueError(f'No support for location_id: {location_id}')
    
    


def is_in_bogota(client, location_id, df_codes = None):
    '''
    Checks if the given location_id is in bogota
    '''
    
    if df_codes is None:
        df_codes = utils.get_geo_codes(client, location_id = None)
        df_codes.index = df_codes.location_id
    
    in_bogota = df_codes.loc[[location_id]].code_depto.apply(lambda c: c not in bogota_codes).sum() == 0
    
    return(in_bogota)


# ---------------------


def get_geo_codes(client, location_id = None):
    '''
    Gets the geo codes of all locations
    '''
    
    if not pd.isna(location_id):
        sql = f'SELECT location_id, code_depto FROM grafos-alcaldia-bogota.geo.locations_geo_codes WHERE location_id = "{location_id}"'
    else:
        sql = f'SELECT location_id, code_depto FROM grafos-alcaldia-bogota.geo.locations_geo_codes'
    
    df_codes =  run_simple_query(client, sql)
    
    return(df_codes)
    


def get_dataset_of_location(client, location_id ):
    '''
    Gets the dataset of the given location id
    '''

    df = get_current_locations(client)
    df.index = df.location_id

    return(df.loc[location_id, 'dataset'])



def get_all_graph_sizes(client):
    '''
    gets all the sizes
    '''
    
    sql = f"""
        
        SELECT location_id, date, num_nodes, num_edges
        FROM grafos-alcaldia-bogota.graph_attributes.graph_sizes
        ORDER BY location_id, date
    
    """

    return( run_simple_query(client, sql))


def get_min_support_date_for_location_attributes(client):
    '''
    Gets the min date that a location is supported
    '''

    sql = f"""
        
        SELECT location_id, min_date
        FROM grafos-alcaldia-bogota.coverage_dates.graphs_min_support_dates
    
    """

    return( run_simple_query(client, sql))

def update_bogota_sample(client, todays_date):
    '''
    Method that updates the bogota (to save money with so many UPZ and Localities)
    '''
    
    table_id = "grafos-alcaldia-bogota.unacast_samples.unacast_bogota"
    
    # Extracts max date
    sql = f"SELECT MAX(date) as max_date FROM {table_id}"
    
    df_temp = run_simple_query(client, sql)
    max_date = df_temp.max_date.values[0]

    if max_date >= todays_date:
        print('      Bogota up to date')
        return(True)
    
    print(f'      Updating Bogota until: {todays_date}')
    
    sql = f"""
        SELECT identifier, timestamp, date, device_lat, device_lon, province_short
        FROM servinf-unacast-prod.unacasttest.unacast_positions_partitioned
        WHERE (province_short = 'CO.33' OR province_short = 'CO.34')
             AND date > "{max_date}" AND date < "{todays_date}"
    
    """
    
    job_config = bigquery.QueryJobConfig(destination = "grafos-alcaldia-bogota.unacast_samples.unacast_bogota", 
                                         write_disposition = 'WRITE_APPEND')
    
    
    query_job = client.query(sql, job_config=job_config)  
    query_job.result()
    
    return(query_job)


def get_housing_code_coverage(client):
    '''
    Gets the max dates for all codes
    '''
    
    
    sql = f"""
            SELECT codes.code_depto as code_depto,
                   max_date       
            FROM
            (SELECT code_depto,
                   MAX(week_date) as max_date
            FROM housing_location.colombia_housing_location 
            GROUP BY code_depto) as loc
            RIGHT JOIN
            (SELECT code_depto
             FROM geo.locations_geo_codes
             GROUP BY code_depto) as codes
             ON codes.code_depto = loc.code_depto
    
    """
    
    return(run_simple_query(client, sql))





def update_housing(client, code_depto, start_date, end_date):
    '''
    Updates Housing for a given code for a given sunday (and the 6 days behind)
    
    '''

    table_id = "grafos-alcaldia-bogota.housing_location.colombia_housing_location"

    if pd.to_datetime(start_date).dayofweek != 0:
        raise ValueError(f'Given Start date is not a monday. Day of week: {pd.to_datetime(start_date).dayofweek}')  
        
    if pd.to_datetime(end_date).dayofweek != 6:
        raise ValueError(f'Given end date is not a sunday. Day of week: {pd.to_datetime(end_date).dayofweek}')    
    
    # SQL Housing
    sql_housing = f"""
    SELECT
          identifier,
          week_date,
          avg_lat as lat,
          avg_lon as lon,
          code_depto,
          "HOUSE" as type
        FROM
          (SELECT
              identifier,
              week_date,
              AVG(CASE WHEN hour < 22 THEN hour + 24 ELSE hour END) as avg_hour, -- Le suma 24 horas si son las horas de la mañana para que el promedio tenga sentido
              STDDEV(CASE WHEN hour < 22 THEN hour + 24 ELSE hour END) as stf_hour, --  Igual pero para la desviacion.
              COUNT(*) as sample_size,
              AVG(device_lat) as avg_lat,
              AVG(device_lon) as avg_lon,
              STDDEV(device_lat) as std_lat,
              STDDEV(device_lon) as std_lon,
              province_short as code_depto,
            FROM
              ( SELECT
              identifier,
              device_lat,
              device_lon,
              DATE_ADD(DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)), INTERVAL 6 DAY) as  week_date, -- Identifica cada semana con su domingo (ultimo dia de la semana)
              EXTRACT( HOUR FROM timestamp) as hour,
              province_short
            FROM
              `servinf-unacast-prod.unacasttest.unacast_positions_partitioned` -- Tabla de la consulta
            WHERE
              province_short = "{code_depto}" 
              AND date >= "{start_date}" AND date <= "{end_date}" -- Lunes y Domingo
              AND device_horizontal_accuracy <= 30) as una 
            WHERE una.hour >= 22 OR una.hour <= 4
            GROUP BY una.identifier, una.week_date, code_depto) as grouped_weeks -- Semanas consolidadas
        WHERE stf_hour >= 0.5 AND sample_size >= 5 AND std_lat <= 10e-4 AND std_lon <= 10e-4 -- Desviacion de 30 minutos entre muestras, al menos 5 puntos y desviacion de 15m entre muestras (en Colombia)

    
    """
    
    # SQL WORK
    sql_work = f"""
    SELECT
          identifier,
          week_date,
          avg_lat as lat,
          avg_lon as lon,
          code_depto,
          "WORK" as type
        FROM
          (SELECT
              identifier,
              week_date,
              AVG(hour) as avg_hour,
              STDDEV(hour) as std_hour,
              STDDEV(week_day) as std_day,
              COUNT(*) as sample_size,
              AVG(device_lat) as avg_lat,
              AVG(device_lon) as avg_lon,
              STDDEV(device_lat) as std_lat,
              STDDEV(device_lon) as std_lon,
              province_short as code_depto,
            FROM
              ( SELECT
              identifier,
              device_lat,
              device_lon,
              EXTRACT(DAYOFWEEK FROM timestamp) as week_day,
              DATE_ADD(DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)), INTERVAL 6 DAY) as  week_date, -- Identifica cada semana con su domingo (ultimo dia de la semana)
              EXTRACT( HOUR FROM timestamp) as hour,
              province_short
            FROM
              `servinf-unacast-prod.unacasttest.unacast_positions_partitioned` -- Tabla de la consulta
            WHERE
              province_short = "{code_depto}" 
              AND date >= "{start_date}" AND date <= "{end_date}" -- Lunes y Domingo
              AND device_horizontal_accuracy <= 30) as una 
            WHERE (una.hour >= 9 AND una.hour <= 12) OR (una.hour >= 2 AND una.hour <= 5)
            GROUP BY una.identifier, una.week_date, code_depto) as grouped_weeks -- Semanas consolidadas
        WHERE std_day > 1 AND std_hour >= 0.5 AND sample_size >= 10 AND std_lat <= 10e-4 AND std_lon <= 10e-4 -- Desviacion de 1 dia entre dias, 30 minutos entre muestras, al menos 10 puntos y desviacion de 15m entre muestras (en Colombia)
    """
    
    # SQL COMMON
    sql_common = f"""
    
        SELECT
          identifier,
          week_date,
          avg_lat as lat,
          avg_lon as lon,
          code_depto,
          "COMMON" as type
        FROM
          (SELECT
              identifier,
              week_date,
              AVG(hour) as avg_hour,
              STDDEV(hour) as std_hour,
              STDDEV(week_day) as std_day,
              COUNT(*) as sample_size,
              AVG(device_lat) as avg_lat,
              AVG(device_lon) as avg_lon,
              STDDEV(device_lat) as std_lat,
              STDDEV(device_lon) as std_lon,
              province_short as code_depto,
            FROM
              ( SELECT
              identifier,
              device_lat,
              device_lon,
              EXTRACT(DAYOFWEEK FROM timestamp) as week_day,
              DATE_ADD(DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)), INTERVAL 6 DAY) as  week_date, -- Identifica cada semana con su domingo (ultimo dia de la semana)
              EXTRACT( HOUR FROM timestamp) as hour,
              province_short
            FROM
              `servinf-unacast-prod.unacasttest.unacast_positions_partitioned` -- Tabla de la consulta
            WHERE
              province_short = "{code_depto}" 
              AND date >= "{start_date}" AND date <= "{end_date}" -- Lunes y Domingo
              AND device_horizontal_accuracy <= 30) as una 
            GROUP BY una.identifier, una.week_date, code_depto) as grouped_weeks -- Semanas consolidadas
        WHERE std_day > 1.5 AND std_hour >= 3 AND sample_size >= 30 AND std_lat <= 10e-4 AND std_lon <= 10e-4 -- Desviacion de 1.5 dias entre dias, 3 horas entre muestras, al menos 10 puntos y desviacion de 15m entre muestras (en Colombia)

    """
    
    job_config = bigquery.QueryJobConfig(destination = table_id, 
                                         write_disposition = 'WRITE_APPEND')
    
    
    # Excecutes
    # Houses
    query_job = client.query(sql_housing, job_config=job_config)
    query_job.result()
    
    # Work
    query_job = client.query(sql_work, job_config=job_config)
    query_job.result()
    
    # Common
    query_job = client.query(sql_common, job_config=job_config) 
    query_job.result()
    
    
    
    return(query_job)



def node_attribute_exists(client, location_id, attribute_name, date):
    '''
    Checks if the attribute is already computed for the given location at the given date.

    date must be in %Y-%m-%d format.
    '''


    query = f"""

        SELECT attribute_name, attribute_value
        FROM {nodes_attribute_table}
        WHERE attribute_name = "{attribute_name}"
              AND location_id = "{location_id}"
              AND date = "{date}"
        LIMIT 1
    """

    df = run_simple_query(client, query)

    return(df.shape[0] > 0)


def graph_attribute_exists(client, location_id, attribute_name, date):
    '''
    Checks if the attribute is already computed for the given location at the given date.

    date must be in %Y-%m-%d format.
    '''


    query = f"""

        SELECT attribute_name, attribute_value
        FROM {graphs_attribute_table}
        WHERE attribute_name = "{attribute_name}"
              AND location_id = "{location_id}"
              AND date = "{date}"
        LIMIT 1
    """

    df = run_simple_query(client, query)

    return(df.shape[0] > 0)



def insert_node_attributes(client, df):
    '''
    Method that appends the nodes attributes

    This method assumes that the columns of the df are the same as the ones in the table
    '''

    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig()

    job = client.load_table_from_dataframe(
        df, nodes_attribute_table, job_config=job_config)

    # Wait for the load job to complete.
    job.result()


def insert_graphs_attributes(client, df):
    '''
    Method that appends the nodes attributes

    This method assumes that the columns of the df are the same as the ones in the table
    '''
    
    
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig()

    job = client.load_table_from_dataframe(
        df, graphs_attribute_table, job_config=job_config)

    # Wait for the load job to complete.
    job.result()



def get_tables_from_dataset(client, dataset_id):    
    '''
    Gets a list of all the tables in a dataset
    
    None if the dataset does not exists
    '''
    
    try:
        dataset = client.get_dataset(f"grafos-alcaldia-bogota.{dataset_id}")  # Make an API request.            
        tables = [t.table_id for t in  list(client.list_tables(dataset))]
        
        return(tables)
    
    except NotFound:
        return None


def get_max_dates_for_graph_attributes(client):
    '''
    Method that gets the max dates of a given attribute
    '''

    sql = f"""
        SELECT attribute_name, location_id, MAX(date) as max_date
        FROM {graphs_attribute_table}
        GROUP BY attribute_name, location_id
    """

    return(run_simple_query(client, sql))


def get_max_dates_for_node_attributes(client):
    '''
    Method that gets the max dates of a given attribute
    '''

    sql = f"""
        SELECT attribute_name, location_id, MAX(date) as max_date
        FROM {nodes_attribute_table}
        GROUP BY attribute_name, location_id
    """

    return(run_simple_query(client, sql))



def get_max_dates_for_graph_statistics(client):
    '''
    Method that gets the max dates of the graph sizes
    '''

    sql = f"""
        SELECT location_id, MAX(date) as max_date
        FROM grafos-alcaldia-bogota.graph_attributes.graph_sizes
        GROUP BY location_id
    """

    return(run_simple_query(client, sql))



def get_max_dates_for_graph_movement(client):
    '''
    Method that gets the max dates of the graph movement
    '''

    sql = f"""
            SELECT location_id, MIN(date) as min_date, MAX(date) as max_date 
            FROM grafos-alcaldia-bogota.graph_attributes.graph_movement
            GROUP BY location_id
            
    """

    return(run_simple_query(client, sql))



def compute_movement(client, location_id, date_string):
    '''
    Computes the movmeent for the given graph
    '''
    
    job_config= bigquery.QueryJobConfig(destination= "grafos-alcaldia-bogota.graph_attributes.graph_movement",
                            write_disposition = 'WRITE_APPEND')
    # Massive Query
    sql = f"""
         WITH   tr as (
                  SELECT DISTINCT identifier
                  FROM grafos-alcaldia-bogota.transits.hourly_transits
                  WHERE date = "{date_string}" AND location_id = "{location_id}"
                  GROUP BY identifier
                ), # --   Transits
                paths as (
                  SELECT p.*
                  FROM grafos-alcaldia-bogota.paths.identifiers_paths as p
                  JOIN tr ON tr.identifier = p.identifier
                  WHERE p.date = "{date_string}"
                ), -- Paths
              labeled_paths as (SELECT identifier,
                 distance,
              ST_DWithin(ST_GeogPoint(paths.lon_1, paths.lat_1), 
                (SELECT geometry FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"), 
                  (SELECT precision FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}")) as start_in,
              ST_DWithin(ST_GeogPoint(paths.lon_2, paths.lat_2), 
                (SELECT geometry FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"), 
                  (SELECT precision FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}")) as end_in      
                FROM paths),
                identifier_positions as (SELECT identifier, NOT (LOGICAL_AND(start_in) AND LOGICAL_AND(end_in)) as traveled
                                         FROM labeled_paths
                                         GROUP BY identifier)



        # Final Select
        SELECT "{location_id}" as location_id,
               DATE("{date_string}") as date,
               all_devices,
               devices_with_movement,
               (all_devices - devices_with_movement) + devices_no_traveled as devices_stayed_in,
               devices_traveled_outside,
               ((all_devices - devices_with_movement) + devices_no_traveled) / (all_devices + 0.00000001) as percentaga_stayed_in,
               devices_traveled_outside / (all_devices + + 0.00000001) as percentaga_treveled_outside,
               all_movement,
               all_movement_avg,
               inner_movement,
               inner_movement_avg,
               outer_movement,
               outer_movement_avg
        FROM (SELECT  # -- Devices
                    (SELECT COUNT(*) FROM tr) as all_devices,
                    (SELECT COUNT(*) FROM identifier_positions) as devices_with_movement,
                    (SELECT COUNT(*) FROM identifier_positions WHERE NOT traveled) devices_no_traveled,
                    (SELECT COUNT(*) FROM identifier_positions WHERE traveled) devices_traveled_outside,

                    # -- All movement
                   (SELECT SUM(distance)/1000
                    FROM labeled_paths) as all_movement,
                   (SELECT AVG(distance)/1000
                    FROM (SELECT SUM(distance) as distance
                           FROM labeled_paths
                           GROUP BY identifier)) as all_movement_avg,

                   # -- Inner Movement
                   (SELECT SUM(distance)/1000
                    FROM labeled_paths
                    WHERE start_in AND end_in) as inner_movement,
                   (SELECT AVG(distance)/1000
                    FROM (SELECT SUM(distance) as distance
                           FROM labeled_paths
                           WHERE start_in AND end_in
                           GROUP BY identifier)) as inner_movement_avg,

                   # -- Outer Movement
                   (SELECT SUM(distance)/1000
                    FROM labeled_paths
                    WHERE NOT start_in AND NOT end_in) as outer_movement,
                   (SELECT AVG(distance)/1000
                    FROM (SELECT SUM(distance) as distance
                           FROM labeled_paths
                           WHERE NOT start_in AND NOT end_in
                           GROUP BY identifier)) as outer_movement_avg)               

    """

    query_job = client.query(sql, job_config= job_config) 
    query_job.result()
    
    return(True)


def create_temp_table(client, table_name, code_depto, start_date, end_date, accuracy = 30):
    '''
    Creates a temporal table with the given table name 
    storing all the results from unacast with the given location id
    '''
    
    table_id = client.dataset(temp_data_set_id).table(table_name)
    schema = [  bigquery.SchemaField("identifier", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("device_lat", "FLOAT"),
                bigquery.SchemaField("device_lon", "FLOAT"),
                bigquery.SchemaField("country_short", "STRING"),
                bigquery.SchemaField("province_short", "STRING"),
                bigquery.SchemaField("device_horizontal_accuracy", "INTEGER")
    ]
    
    # Deletes if exists
    client.delete_table(table_id, not_found_ok=True)
    
    # Creates
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_= bigquery.TimePartitioningType.DAY,
        field="timestamp",  # name of column to use for partitioning
    )  
    table.clustering_fields = ['device_horizontal_accuracy']
    table = client.create_table(table)

    print("   Creating table {}, partitioned on column {} for depto_code: {}".format(table.table_id, table.time_partitioning.field, code_depto))
    
    job_config = bigquery.QueryJobConfig(destination= table_id,
                                        write_disposition = 'WRITE_APPEND')
    
    # SQL statement
    sql = f"""
        SELECT identifier, TIMESTAMP(timestamp) as timestamp, device_lat, device_lon, country_short, province_short, device_horizontal_accuracy
        FROM `servinf-unacast-prod.unacasttest.unacast_positions_partitioned`
        WHERE province_short = "{code_depto}"
              AND device_horizontal_accuracy <= {accuracy}
              AND date >= DATE("{start_date}") 
              AND date <= DATE("{end_date}");
    """
    
    # Start the query, passing in the extra configuration.
    start = time.time()
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    res = query_job.result()  # Wait for the job to complete.

    print("      Done. Records for {} loaded to the table {}. Time: {}m".format(code_depto, table_name, np.round((time.time() - start)/60, 2)))

    return(res)


def get_totals_by_hour(client, table_id):
    '''
    Return a pandas DataFrame with the total by hour of the dataframe
    '''
    
    job_config = bigquery.QueryJobConfig()
        
    
    sql = f"""
    SELECT date, COUNT(*) as total
    FROM (
      SELECT TIMESTAMP_TRUNC(timestamp, HOUR) AS date
      FROM {table_id}) AS t
    GROUP BY t.date
    ORDER BY t.date
    """
    
    query_job = client.query(sql, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    return(df)



def append_by_time_window(client, start_timestamp, end_timestamp, code_depto, source_table_id, destination_table_id, accuracy, hours):
    '''
    Methods that appends contact results from the selected time window


    # New SQL Query
    -- Divides by 5 minute timewindow
    -- Includes min accuracy
    -- Includes avg and min distance
    -- Includes avg and min time_ 


        -- OLD QUERY

      WITH selected as
        (
          SELECT identifier, timestamp, device_lon, device_lat, province_short, device_horizontal_accuracy
          FROM {source_table_id}
          WHERE province_short = "{code_depto}"
          AND device_horizontal_accuracy <= {accuracy}
          AND timestamp >= TIMESTAMP("{start_timestamp}") 
          AND timestamp < TIMESTAMP("{end_timestamp}")
        )

        SELECT 
            id1, 
            id2, 
            date,
            hour,
            code_depto,
            AVG(lat) as lat,
            AVG(lon) as lon,
            AVG(id1_device_accuracy) as id1_device_accuracy,
            AVG(id2_device_accuracy) as id2_device_accuracy,
            COUNT(*) as contacts
        FROM
        (        
            SELECT
              unacast.identifier AS id1,
              unacast2.identifier AS id2,
              DATE(unacast.timestamp) AS date,
              EXTRACT(HOUR FROM unacast.timestamp) AS hour,
              unacast.province_short AS code_depto,
              unacast.device_lat as lat,
              unacast.device_lon as lon,
              unacast.device_horizontal_accuracy as id1_device_accuracy,
              unacast2.device_horizontal_accuracy as id2_device_accuracy
            FROM
               selected AS unacast,
               selected AS unacast2
            WHERE              
                  unacast.identifier < unacast2.identifier
              AND ABS(CAST(TIMESTAMP_DIFF(unacast.timestamp, unacast2.timestamp, Minute) AS int64)) <= 2
              AND ST_DWITHIN(ST_GeogPoint(unacast.device_lon, unacast.device_lat), ST_GeogPoint(unacast2.device_lon, unacast2.device_lat),2)
        ) 
        GROUP BY date, hour, code_depto, id1, id2

    '''
    
    job_config = bigquery.QueryJobConfig(destination = destination_table_id, 
                                         write_disposition = 'WRITE_APPEND')
        
    sql = f"""
    
        WITH selected as
        (
          SELECT identifier, timestamp, device_lon, device_lat, province_short, device_horizontal_accuracy
          FROM {source_table_id}
          WHERE province_short = "{code_depto}"
          AND device_horizontal_accuracy <= {accuracy}
          AND timestamp >= TIMESTAMP("{start_timestamp}") 
          AND timestamp < TIMESTAMP("{end_timestamp}")
        )

        SELECT 
            id1, 
            id2, 
            date,
            hour,
            minute,
            code_depto,
            AVG(lat) as lat,
            AVG(lon) as lon,
            AVG(id1_device_accuracy) as avg_id1_device_accuracy,
            AVG(id2_device_accuracy) as avg_id2_device_accuracy,
            MIN(id1_device_accuracy) as min_id1_device_accuracy,
            MIN(id2_device_accuracy) as min_id2_device_accuracy,
            AVG(distance) as avg_distance,
            MIN(distance) as min_distance,
            AVG(time_difference) as avg_time_difference,
            MIN(time_difference) as min_time_difference,
            COUNT(*) as contacts
        FROM
        (        
            SELECT
              unacast.identifier AS id1,
              unacast2.identifier AS id2,
              DATE(unacast.timestamp) AS date,
              EXTRACT(HOUR FROM unacast.timestamp) AS hour,
              CAST(5*FLOOR(EXTRACT(MINUTE FROM unacast.timestamp)/5) AS INT64) AS minute,
              unacast.province_short AS code_depto,
              unacast.device_lat as lat,
              unacast.device_lon as lon,
              unacast.device_horizontal_accuracy as id1_device_accuracy,
              unacast2.device_horizontal_accuracy as id2_device_accuracy,
              ST_DISTANCE(ST_GeogPoint(unacast.device_lon, unacast.device_lat), ST_GeogPoint(unacast2.device_lon, unacast2.device_lat)) as distance,
              ABS(CAST(TIMESTAMP_DIFF(unacast.timestamp, unacast2.timestamp, Minute) AS int64)) as time_difference
            FROM
               selected AS unacast,
               selected AS unacast2
            WHERE              
                  unacast.identifier < unacast2.identifier
              AND ABS(CAST(TIMESTAMP_DIFF(unacast.timestamp, unacast2.timestamp, Minute) AS int64)) <= 2
              AND ST_DISTANCE(ST_GeogPoint(unacast.device_lon, unacast.device_lat), ST_GeogPoint(unacast2.device_lon, unacast2.device_lat)) < 5
        ) 
        GROUP BY date, hour, minute, code_depto, id1, id2
    """
    

    query_job = client.query(sql, job_config=job_config)  
    query_job.result()
    
    return(query_job)



def add_code_deptos(client, location_id):
    '''
    Method that adds the depto codes for the given location id
    '''
    
    sql = f"""
        SELECT location_id
        FROM grafos-alcaldia-bogota.geo.locations_geo_codes
        GROUP BY location_id
    """
    
    df_locations = run_simple_query(client, sql)
    
    if location_id in df_locations.location_id:
        print('Location id: {} is already in the locations_geo_codes table. Nothing will be added')
        return(False)
    


    query = f"""
            SELECT
              "{location_id}" as location_id,
              province_short as code_depto
            FROM
              `servinf-unacast-prod.unacasttest.unacast_positions_partitioned` AS unacast
            WHERE
              date >= "2020-04-01" AND date <= "2020-04-03"
              AND ST_DWithin(ST_GeogPoint(unacast.device_lon,
                  unacast.device_lat),
                (SELECT geometry FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"),
                1000)
            GROUP BY  province_short
    """
    
    job_config= bigquery.QueryJobConfig(destination= "grafos-alcaldia-bogota.geo.locations_geo_codes",
                                        write_disposition = 'WRITE_APPEND')
    
    query_job = client.query(query, job_config= job_config) 
    query_job.result()
        
    return(True)


def get_locations_with_geo_codes(client):
    
    
    sql = f"""
        SELECT location_id, COUNT(*) num_codes
        FROM grafos-alcaldia-bogota.geo.locations_geo_codes
        GROUP BY location_id
    """
    
    df_locations = run_simple_query(client, sql)
    
    return(df_locations)
    
    
    

def compute_transits(client, location_id, start_date, end_date, ident = '   '):
    '''
    Computes the identifiers transits for the given location between the dates. End date is not inclusive
    '''
    
    print(ident + f'Adding transtits for: {location_id}. Between: {start_date} and {end_date}')
        
    df_codes =  get_geo_codes(client, location_id)
    
    
    if df_codes.code_depto.apply(lambda c: c not in bogota_codes).sum() == 0:
        
        print('      Inside Bogota')
        query = f"""
        SELECT
          "{location_id}" as location_id,
          identifier,
          date,
          EXTRACT(HOUR FROM unacast.timestamp) AS hour,
          COUNT(*) total_transits
        FROM
          grafos-alcaldia-bogota.unacast_samples.unacast_bogota AS unacast
        WHERE
          date >= "{start_date}" AND date < "{end_date}"
          AND ST_DWithin(ST_GeogPoint(unacast.device_lon,
              unacast.device_lat),
            (SELECT geometry FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"),
            (SELECT precision FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"))
        GROUP BY  date, EXTRACT(HOUR FROM unacast.timestamp), identifier
        """        
    else:
        query = f"""
        SELECT
          "{location_id}" as location_id,
          identifier,
          date,
          EXTRACT(HOUR FROM unacast.timestamp) AS hour,
          COUNT(*) total_transits
        FROM
          `servinf-unacast-prod.unacasttest.unacast_positions_partitioned` AS unacast
        WHERE
          date >= "{start_date}" AND date < "{end_date}"
          AND ST_DWithin(ST_GeogPoint(unacast.device_lon,
              unacast.device_lat),
            (SELECT geometry FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"),
            (SELECT precision FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"))
        GROUP BY  date, EXTRACT(HOUR FROM unacast.timestamp), identifier
        """
    
    
    job_config= bigquery.QueryJobConfig(destination= "grafos-alcaldia-bogota.transits.hourly_transits",
                                        write_disposition = 'WRITE_APPEND')
    
    query_job = client.query(query, job_config= job_config) 
    query_job.result()
    
    print(ident + 'Done')
    
    return('OK')


def get_transits_coverage(client):
    '''
    Gets the transit coverage of the different location ids in the data base
    '''
    
    job_config = bigquery.QueryJobConfig()
        
    
    sql = f"""        
        SELECT location_id, MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as total_transits
        FROM `grafos-alcaldia-bogota.transits.hourly_transits` 
        GROUP BY location_id
    """
    
    query_job = client.query(sql, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    return(df)



def get_active_depto_codes(client):
    '''
    Gets the depto codes that have at least one active polygon.
    
    Returns an array with the active depto codes
    '''
    
    job_config = bigquery.QueryJobConfig()
        
    
    sql = f"""
        SELECT code_depto
        FROM
        (SELECT location_id, code_depto 
        FROM grafos-alcaldia-bogota.geo.locations_geo_codes
        ) as t1
        JOIN
        (SELECT location_id
        FROM grafos-alcaldia-bogota.geo.locations_geometries
        WHERE active OR attribute_active) as t2
        ON t1.location_id = t2.location_id
        GROUP BY code_depto
        """
    
    query_job = client.query(sql, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    return(df.code_depto.values)

def get_coverage_of_depto_codes(client):
    '''
    Return a pandas DataFrame with the coverage of dates for each code_depto for the 
    contacts by hour table

    Original Query
  
        SELECT t1.code_depto, min_date, max_date
        FROM
        (SELECT code_depto, COUNT(*) as total_locations
        FROM grafos-alcaldia-bogota.geo.locations_geo_codes
        GROUP BY code_depto) as t1
        LEFT JOIN
        (SELECT code_depto, COUNT(*) as total_contacts, MIN(DATETIME_ADD(DATETIME(date), INTERVAL hour HOUR)) as min_date, MAX(DATETIME_ADD(DATETIME(date), INTERVAL hour HOUR)) as max_date
        FROM grafos-alcaldia-bogota.contactos_hour.all_locations
        GROUP BY code_depto) as t2
        ON t1.code_depto = t2.code_depto
    
    Converted into a single table that must be be keeped up to date.
    '''
    
    job_config = bigquery.QueryJobConfig()
        
    
    sql = f"""
        SELECT t1.code_depto, min_date, max_date
        FROM
        (SELECT code_depto, COUNT(*) as total_locations
        FROM grafos-alcaldia-bogota.geo.locations_geo_codes
        GROUP BY code_depto) as t1
        LEFT JOIN
        (SELECT code_depto, min_date, max_date
        FROM grafos-alcaldia-bogota.coverage_dates.contacts_coverage) as t2
        ON t1.code_depto = t2.code_depto
    """
    
    query_job = client.query(sql, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
        
    df.min_date = pd.to_datetime(df.min_date, errors='coerce')
    df.max_date = pd.to_datetime(df.max_date, errors='coerce')    
    
    df.min_date = pd.to_datetime( df.min_date.apply(lambda d: d.strftime(long_date_format)), errors='coerce')
    df.max_date = pd.to_datetime( df.max_date.apply(lambda d: d.strftime(long_date_format)), errors='coerce')
    
    return(df)


def save_contacts_coverage(client, df_new):
    '''
    Method that saves the contacts coverage, overwritting the current implementation.

    Preforms a consisty check over the given coverage to ensure that it can overwritten and not
    loose data integrity

    '''

    df_old = get_coverage_of_depto_codes(client)

    # Columns
    if not df_old.columns.equals(df_new.columns):

      df_new.to_csv(f'inconsistent_conctacts_coverage_{get_today()}.csv', index = False)
      raise ValueError("""Error when saving contacts coverage, the new coverage does not have the same columns.
                          DataFrame will be saved in excecution directory to be manually corrected and uploaded.""")

    # Assigns the index
    df_old.index = df_old.code_depto
    df_new.index = df_new.code_depto

    # Indexes
    if not df_old.index.equals(df_new.index):

      df_new.to_csv(f'inconsistent_contacts_coverage_{get_today()}.csv', index = False)
      raise ValueError("""Error when saving contacts coverage, the new coverage does not have the same rows.
                          DataFrame will be saved in excecution directory to be manually corrected and uploaded.""")
   
    # Sorts
    df_new = df_new.loc[df_old.index]

    # Null dates
    if df_new.min_date.isna().sum() > 0 or df_new.min_date.isna().sum() > 0:
      
      df_new.to_csv(f'inconsistent_contacts_coverage_{get_today()}.csv', index = False)
      raise ValueError("""Error when saving contacts coverage: Min and Max dates cannot be None.
                          DataFrame will be saved in excecution directory to be manually corrected and uploaded.""")

    # Max >= Min
    if (df_new.min_date > df_new.max_date).sum() > 0:
      
      df_new.to_csv(f'inconsistent_contacts_coverage_{get_today()}.csv', index = False)
      raise ValueError("""Error when saving contacts coverage: Max date must be greater or equal to Min date.
                          DataFrame will be saved in excecution directory to be manually corrected and uploaded.""")
    # old min == new min
    if (df_new[~df_old.min_date.isna()].min_date != df_old[~df_old.min_date.isna()].min_date ).sum() > 0:
      
      df_new.to_csv(f'inconsistent_contacts_coverage_{get_today()}.csv', index = False)
      raise ValueError("""Error when saving contacts coverage: Min dates should not be changed.
                          DataFrame will be saved in excecution directory to be manually corrected and uploaded.""")

    # old min == new min
    if (df_new[~df_old.max_date.isna()].max_date < df_old[~df_old.max_date.isna()].max_date ).sum() > 0:
      
      df_new.to_csv(f'inconsistent_contacts_coverage_{get_today()}.csv', index = False)
      raise ValueError("""Error when saving contacts coverage: Max date should only increase
                          DataFrame will be saved in excecution directory to be manually corrected and uploaded.""")
    

    # Consistency checked
    # Saves the new coverage

    # resets index
    df_coverage = df_new.reset_index(drop = True)
    
    table_id = 'grafos-alcaldia-bogota.coverage_dates.contacts_coverage'
    
    
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("code_depto", "STRING"),
        bigquery.SchemaField("min_date", "TIMESTAMP"),
        bigquery.SchemaField("max_date", "TIMESTAMP"),
    ],write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        df_coverage, table_id, job_config=job_config
    )

    # Wait for the load job to complete.
    job.result()



def get_depto_code_seniority_coverage(client):
    '''
    Method that extracts the code depto coverage for identifier seniority.
    Extracts all depto_codes in use (even non active ones)
    '''


    query = """

        SELECT codes.code_depto as code_depto, sen.max_date as max_date 
        FROM
        (SELECT code_depto
        FROM `grafos-alcaldia-bogota.geo.locations_geo_codes`
        GROUP BY code_depto) AS codes
        LEFT JOIN 
        (SELECT code_depto, MAX(date) as max_date
        FROM `grafos-alcaldia-bogota.seniority.identifier_seniority`
        GROUP BY code_depto) AS sen 
        ON sen.code_depto = codes.code_depto


    """

    return(run_simple_query(client, query))


def update_seniority(client, code_depto, start_time, end_time):
    """
    Updates seniority for the given parameters
    NOTE: Start date paremeter is inclive and end date parameter is not
    """


    sql = f"""
        SELECT province_short AS code_depto, identifier, date
        FROM `servinf-unacast-prod.unacasttest.unacast_positions_partitioned`
        WHERE province_short = "{code_depto}" 
            AND date >= "{start_time.strftime(date_format)}"
            AND date < "{end_time.strftime(date_format)}"
        GROUP BY province_short, identifier, date
    
    """
    
    job_config = bigquery.QueryJobConfig(destination = "grafos-alcaldia-bogota.seniority.identifier_seniority", 
                                         write_disposition = 'WRITE_APPEND')
    
    
    query_job = client.query(sql, job_config=job_config)  
    query_job.result()
    
    return(query_job)





def get_path_coverage_of_depto_codes(client):
    '''
    Return a pandas DataFrame with the coverage of dates for each code_depto for the 
    different paths
    '''
    
    job_config = bigquery.QueryJobConfig()
        
    
    sql = f"""
        SELECT t1.code_depto, total_locations, total_points, min_date, max_date
        FROM
        (SELECT code_depto, COUNT(*) as total_locations
        FROM grafos-alcaldia-bogota.geo.locations_geo_codes
        GROUP BY code_depto) as t1
        LEFT JOIN
        (SELECT code_depto, COUNT(*) as total_points, MIN(date) as min_date, MAX(date) as max_date
        FROM grafos-alcaldia-bogota.paths.identifiers_paths
        GROUP BY code_depto) as t2
        ON t1.code_depto = t2.code_depto
    """
    
    query_job = client.query(sql, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    return(df)



def add_paths_for_code_on_date(client, code_depto, date, accuracy = global_accuracy):
    '''
    Method that updates the paths for the given attributes
    '''
    
    destination_table_id = f"grafos-alcaldia-bogota.paths.identifiers_paths"
    job_config = bigquery.QueryJobConfig(destination = destination_table_id, 
                                         write_disposition = 'WRITE_APPEND')
        
    sql = f"""
    
            WITH sorted_devices as (SELECT
                                      "{code_depto}" as code_depto,
                                      identifier,
                                      date,
                                      EXTRACT(HOUR FROM MAX(unacast.timestamp)) AS hour,
                                      MAX(timestamp) as timestamp,
                                      unacast.device_lon as lon,
                                      unacast.device_lat as lat,
                                      ROW_NUMBER() OVER(ORDER BY identifier, MAX(timestamp)) AS row_num
                                    FROM
                                      `servinf-unacast-prod.unacasttest.unacast_positions_partitioned` AS unacast
                                    WHERE
                                      date = "{date}"
                                      AND device_horizontal_accuracy <= {accuracy}
                                      AND province_short = "{code_depto}"
                                    GROUP BY identifier, date, unacast.device_lon, unacast.device_lat),
                 next_positions as (SELECT s1.code_depto as code_depto, 
                                           s1.identifier as identifier, 
                                           s1.date as date, 
                                           s1.hour as hour_1, 
                                           s2.hour as hour_2, 
                                           s1.timestamp as t1, 
                                           s2.timestamp as t2,
                                           s1.lon as lon_1,
                                           s1.lat as lat_1, 
                                           s2.lon as lon_2, 
                                           s2.lat as lat_2
                                    FROM sorted_devices as s1
                                    JOIN sorted_devices as s2
                                    ON s1.row_num +1 = s2.row_num 
                                        AND s1.identifier = s2.identifier
                                        AND s1.timestamp <> s2.timestamp
                                    ORDER BY s1.row_num, identifier, t1, t2)

            SELECT code_depto, 
                    identifier, 
                    date,
                    hour_1, 
                    hour_2,
                    t1, 
                    t2, 
                    lon_1,
                    lat_1, 
                    lon_2, 
                    lat_2,
                    distance,
                    minutes,
                    distance / minutes as velocity
            FROM
            (
                SELECT *,
                       ST_DISTANCE(ST_GEOGPOINT(nx.lon_1, nx.lat_1), ST_GEOGPOINT(nx.lon_2, nx.lat_2)) as distance, 
                       DATETIME_DIFF(nx.t2, nx.t1, MINUTE) as minutes
                FROM next_positions as nx
            ) WHERE minutes > 30 OR (minutes > 0 AND distance / (minutes + 0.0000001) < 40) # 150 km/h
    """
    

    query_job = client.query(sql, job_config=job_config)  
    query_job.result()
    
    return(query_job)
    




def update_contacts_for_depto_code(client, code_depto, start_time, end_time,
                                    jump = 12, # Hours
                                    treshold = 30, # Time Treshold Seconds
                                    accuracy = 30, # Start accuracy                    
                                    max_jump = 12, # Max jump (hours)
                                    max_accuracy = 30, # Max Accuracy
                                    accuracy_jump = 2, # The accuracy jump
                                    verbose = True):
    '''
    Updates the contacts table for a single code_depto for the times stamps given.
    
    args:
        code_depto (str): Geographical province conde for the Unicast dataset
        start_time (pandas Timestamp): start date stamp
        end_time (pandas Timestamp): end date stamp (non inclusive)
        jump (int): start jump in hours
        treshold (int): treshold in second for the jump and accuracy adjustments
        accuracy (int): start accuracy of the extraction
        max_accuracy (int): The maximum accuracy to consider. Note: The temporal table is created with <= 30, so anything above that will not alter the results
        accuracy_jump (int): The accuracy jumps to take as the algorithm asjusts
        verbose (boolean)
    '''
    ident = '   '
    
    file_name = 'progress/progresss_{}.csv'.format(code_depto)
    
    # Creates flder if does not exists
    if not os.path.exists('progress'):
        os.makedirs('progress')
    
    if not os.path.isfile(file_name):
        with open(file_name, 'w') as f:
            f.write("start_timestamp,end_timestamp,window_size,accuracy,time_s,ellapsed_m,skipped" + "\n")
            
    # Min Jump
    min_jump = 1
    
    # Computes dates
    start_date = start_time.strftime('%Y-%m-%d')
    end_date = end_time.strftime('%Y-%m-%d')
    
    # Creates the temporal table
    temp_table_name = 'contactos_temp_table'
    
    # Creates the temporal table
    res = create_temp_table(client, temp_table_name, code_depto, start_date, end_date)

    # Name of sources
    source_table_id = 'grafos-alcaldia-bogota.{}.{}'.format(temp_data_set_id, temp_table_name)
    destination_table_id = 'grafos-alcaldia-bogota.contacts_minute.all_locations'



    process_start = time.time()
    if verbose:
        print(ident + 'Started')
    

    start_timestamp = start_time
    while start_timestamp < end_time:
        end_timestamp = min(start_timestamp + timedelta(hours = jump), end_time)

        s = time.time()
        try:
            append_by_time_window(client = client,
                                  start_timestamp = str(start_timestamp), 
                                  end_timestamp = str(end_timestamp),
                                  code_depto = code_depto,
                                  source_table_id =  source_table_id, 
                                  destination_table_id = destination_table_id,
                                  accuracy = accuracy,
                                  hours = jump)
        except BadRequest as e:
            
            #print(e)

            if jump > min_jump:
                jump = min_jump
            else:
                accuracy = max(0,accuracy - accuracy_jump)
                        
            if accuracy == 0:
                if verbose:
                    print(f'      Limit reached. Skipping date: {start_timestamp}')

                row = f'{start_timestamp},{end_timestamp},{jump},{accuracy},{np.round(elapsed,2)},{np.round( (time.time() - process_start)/60,1)},{accuracy == 0}'
                with open(file_name, 'a') as f:
                    f.write(row + '\n')

                start_timestamp = end_timestamp
                accuracy += accuracy_jump

            else:
                print(ident + f'      Max time exceeded on {start_timestamp} to {end_timestamp} . Adjusting. Jump: {jump}, accuracy: {accuracy} and will retry.')

            time.sleep(5)
            continue

        elapsed = time.time() - s

        row = f'{start_timestamp},{end_timestamp},{jump},{accuracy},{np.round(elapsed,2)},{np.round( (time.time() - process_start)/60,1)},{accuracy == 0}'
        print(ident + f'   Interval: From  {start_timestamp} to {end_timestamp}. Jump: {jump} hours, Accuracy: {accuracy}. Excecution Time: {np.round(elapsed,2)}s. Time Since Start: {np.round( (time.time() - process_start)/60,1)}m')


        with open(file_name, 'a') as f:
            f.write(row + '\n')

        if elapsed < treshold:
            if accuracy < max_accuracy:
                accuracy += accuracy_jump
            elif jump < max_jump:
                jump += 1

        if elapsed > treshold:
            if jump > min_jump:
                jump -= 1 
            else:
                accuracy = max(0,accuracy - accuracy_jump)


        # Advances
        start_timestamp = end_timestamp
        
    if verbose:
        print()
        print(ident + 'Done')
        print(ident + f'Total time: {np.round((time.time() - process_start)/60, 2)} minutes')
        print('-------------')
        print('')
        
        
    


def get_max_date_for_graph_table(client, dataset, graph_name, min_date = None):
    '''
    Method that extracts the start date and end date for a given graphs table
    '''
    job_config = bigquery.QueryJobConfig()
     
    if pd.isna(min_date):
        sql = f"""
         SELECT MAX(date) as max_date
            FROM grafos-alcaldia-bogota.{dataset}.{graph_name}
        """
    else:        
        sql = f"""
         SELECT MAX(date) as max_date
            FROM grafos-alcaldia-bogota.{dataset}.{graph_name}
            WHERE date >= "{min_date}"
        """
        
    query_job = client.query(sql, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    max_date = df.max_date.iloc[0]
    
    if pd.isna(max_date) and not pd.isna(min_date):
        print(f'Error in: {graph_name}. No max date found after: {min_date}. Will compute from start.')
        return(get_max_date_for_graph_table(client, dataset, graph_name))

        
    return(max_date)

def get_min_date_for_graph_table(client, dataset, graph_name):
    '''
    Method that extracts the start date and end date for a given graphs table
    '''
    job_config = bigquery.QueryJobConfig()
     

    sql = f"""
         SELECT MIN(date) as min_date
            FROM grafos-alcaldia-bogota.{dataset}.{graph_name}
        """
    query_job = client.query(sql, job_config=job_config) 

    # Return the results as a pandas DataFrame
    df = query_job.to_dataframe()
    
    return(df.min_date.iloc[0])


def add_edglists_to_graph(client, dataset_id, graph_name, date):
    '''
    Method that adds the edges of the corresponding dates
    '''
    
    destination_table_id = f"grafos-alcaldia-bogota.{dataset_id}.{graph_name}"
    job_config = bigquery.QueryJobConfig(destination = destination_table_id, 
                                         write_disposition = 'WRITE_APPEND')
        
    sql = f"""
    
        WITH tr as (
          SELECT DISTINCT identifier
          FROM grafos-alcaldia-bogota.transits.hourly_transits
          WHERE date = "{date}" AND location_id = "{graph_name}"
          GROUP BY identifier
        ), 
        contacts as (
          SELECT con.* 
          FROM grafos-alcaldia-bogota.contacts_minute.all_locations as con
          JOIN (SELECT code_depto FROM grafos-alcaldia-bogota.geo.locations_geo_codes WHERE location_id = "{graph_name}") codes
          ON con.code_depto = codes.code_depto
          WHERE con.date = "{date}"
        )


        SELECT filtered_contacts.*
        FROM 
        (SELECT contacts.*
          FROM contacts
          JOIN tr
          ON contacts.id1 = tr.identifier) as filtered_contacts
        JOIN tr
        ON filtered_contacts.id2 = tr.identifier

    """
    

    query_job = client.query(sql, job_config=job_config)  
    query_job.result()
    
    return(query_job)


def create_dataset(client, dataset_id):
    '''
    Method that creates a given dataset
    '''
    
    # Creates
    try:
        dataset = client.create_dataset(dataset_id) 
    except:
        pass

def create_edgelist_table(client, dataset, graph_id):
    '''
    Method that creates a a graph table
    '''
    
    table_id = client.dataset(dataset).table(graph_id)
    schema = [  bigquery.SchemaField("id1", "STRING"),
                bigquery.SchemaField("id2", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("hour", "INTEGER"),
                bigquery.SchemaField("minute", "INTEGER"),
                bigquery.SchemaField("code_depto", "STRING"),
                bigquery.SchemaField("lat", "FLOAT"),
                bigquery.SchemaField("lon", "FLOAT"),            
                bigquery.SchemaField("avg_id1_device_accuracy", "FLOAT"),
                bigquery.SchemaField("avg_id2_device_accuracy", "FLOAT"),
                bigquery.SchemaField("min_id1_device_accuracy", "INTEGER"),
                bigquery.SchemaField("min_id2_device_accuracy", "INTEGER"),
                bigquery.SchemaField("avg_distance", "FLOAT"),
                bigquery.SchemaField("min_distance", "FLOAT"),
                bigquery.SchemaField("avg_time_difference", "FLOAT"),
                bigquery.SchemaField("min_time_difference", "INTEGER"),
                bigquery.SchemaField("contacts", "INTEGER")
    ]

    # Creates
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_= bigquery.TimePartitioningType.DAY,
        field="date",  # name of column to use for partitioning
    ) 
    table.clustering_fields = ['code_depto']

    # Creates
    table = client.create_table(table)

    
    
def get_last_graph_coverage(client):
    '''
    Gets the last saved graph coverage
    '''
    
    job_config = bigquery.QueryJobConfig()
    
    table_id = 'grafos-alcaldia-bogota.coverage_dates.graphs_coverage'
    
    sql = f"""
          SELECT *
          FROM {table_id}
            """
    
    try:
        query_job = client.query(sql, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()
        return(df)
    except:
        return(None)
    

    
def refresh_graphs_coverage(client, df_coverage):
    '''
    Updates the graphs coverage
    '''
        
    # resets index
    df_coverage = df_coverage.reset_index(drop = True)
    
    # substracts one day for safety
    df_coverage.end_date = df_coverage.end_date - timedelta(days = 1)
    
    table_id = 'grafos-alcaldia-bogota.coverage_dates.graphs_coverage'
    # deletes the table
    client.delete_table(table_id, not_found_ok=True)
    
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("location_id", "STRING"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("end_date", "DATE"),
    ])

    job = client.load_table_from_dataframe(
        df_coverage, table_id, job_config=job_config
    )

    # Wait for the load job to complete.
    job.result()
    
    
    
def get_edgelists_coverage(client):
    
    # gets locations
    df_locations = get_current_locations(client)
    
    # Starts the dates
    df_locations['start_date'] = None
    df_locations['end_date'] = None
    df_locations['dataset_exists'] = False
    df_locations['table_exists'] = False
    
    # Sets the index
    df_locations.index = df_locations.location_id
    
    # Sets the minimum date
    
        
    # Loads old coverage (to bring costs down)
    df_old_coverage = get_last_graph_coverage(client)
    
    if df_old_coverage is not None:
        for ind, row in df_old_coverage.iterrows():
            if row.location_id in df_locations.location_id:
                df_locations.loc[row.location_id,'start_date'] = row.start_date
                df_locations.loc[row.location_id,'end_date'] = row.end_date
            
    # Extracts final dates
    for ind, row in df_locations.iterrows():
        
        
        # extracts tables        
        tables = get_tables_from_dataset(client, row.dataset)
        if tables is None:
            # No dataset found
            continue
        
        # Dataset exists
        df_locations.loc[ind, 'dataset_exists'] = True

        # If table exists
        if row.location_id in tables:

            # Sets parameter
            df_locations.loc[ind, 'table_exists'] = True

            # Extracts min date (if missing)
            if pd.isna(df_locations.loc[ind,'start_date']):
                min_date = get_min_date_for_graph_table(client, row.dataset, row.location_id)
                df_locations.loc[ind,'start_date'] = min_date


            # Extracts max date
            max_date = get_max_date_for_graph_table(client, row.dataset, row.location_id, df_locations.loc[ind,'end_date'])
            df_locations.loc[ind,'end_date'] = max_date
            # Updates                
    
    
    # Checks for consistency
    with_error = df_locations[(df_locations.start_date.isna()) | (df_locations.end_date.isna())]
    with_error = with_error[~((with_error.start_date.isna()) & (with_error.end_date.isna()))]
    
    if with_error.shape[0] > 0:
        print(with_error)
        raise ValueError('Start Date and end date must both be none or none of them')
    
    return(df_locations)


# The gini index function
# Source: from https://github.com/oliviaguest/gini

def gini(array):
    """Calculate the Gini coefficient of a numpy array."""
    # Method copied from https://github.com/oliviaguest/gini
    
    array = np.array(array).flatten() #all values are treated equally, arrays must be 1d
    if np.amin(array) < 0:
        array -= np.amin(array) #values cannot be negative
    array = array + 0.0000001 #values cannot be 0
    array = np.sort(array) #values must be sorted
    index = np.arange(1,array.shape[0]+1) #index per array element
    n = array.shape[0]#number of array elements
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array))) #Gini coefficient

################# FUNCTIONS FROM NOTES #####################

def compute_personalized_pagerank(nodes, edges, weighted):
    '''
    Computes personalized pagerank. Nodes must have weight attribute
    '''
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
    
    if weighted:
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the personalized pagerank
        personalized_page_rank = G.personalized_pagerank(weights = 'weight', directed = False, reset = nodes['dist_weight'].values)
    
    else:
        personalized_page_rank = G.personalized_pagerank(directed = False, reset = nodes['dist_weight'].values)
        
    # Returns the value
    return(personalized_page_rank)


def compute_pagerank(nodes, edges, weighted):
    
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
    
    if weighted:
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the personalized pagerank
        page_rank = G.pagerank(weights = 'weight', directed = False)
    
    else:
        page_rank = G.pagerank(directed = False)
        
    # Returns the value
    return(page_rank)    


    
def compute_eigenvector(nodes, edges, weighted):
    '''
    Computes eigen vector
    '''
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
    
    if weighted:
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the personalized pagerank
        eigen_vector = G.evcent(weights = 'weight', directed = False)
    
    else:
        # Exctracs the personalized pagerank
        eigen_vector = G.evcent(directed = False)
    
    # Returns the value
    return(eigen_vector)    
    
    
def compute_eigenvalue(nodes, edges, weighted):
    '''
    Computes personalized pagerank.
    '''
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))

    if weighted:        
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the eigen value
        _, eigen_value = G.evcent(weights = 'weight', directed = False, return_eigenvalue = True)

    else:

        # Exctracs the eigen value
        _, eigen_value = G.evcent(directed = False, return_eigenvalue = True)
        
    # Returns the value
    return(eigen_value)


def sort_edges_by_centrality(nodes, edges, centrality):
    
    df_centrality = pd.DataFrame({'val':centrality}, index = nodes.identifier)
    
    new_edges = edges.copy()
    new_edges['impact'] = df_centrality.loc[new_edges.id1, 'val'].values*df_centrality.loc[new_edges.id2, 'val'].values
    new_edges.sort_values('impact', ascending = False, inplace = True)
    
    return(new_edges)


def sort_edges(nodes, edges, method):
    '''
    Sorts the edges
    
    '''
    
    if method.upper() == 'RANDOM':
        return(edges.sample(edges.shape[0]))
    
    if method.upper() == 'EIGENVECTOR':
        
        centrality = compute_eigenvector(nodes, edges, weighted = False)
        return(sort_edges_by_centrality(nodes, edges, centrality))
    
    if method.upper() == 'PAGERANK':
        
        centrality = compute_pagerank(nodes, edges, weighted)
        return(sort_edges_by_centrality(nodes, edges, centrality))
    
    
    raise ValueError(f'Unsupported Method: {method}')
    
    
def get_edges(dataset_id, location_id, start_date, end_date ):
    
    
    # Edges 
    query = f"""
            SELECT id1, id2, COUNT(*) as weight, SUM(contacts) as total_contacts
            FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
            WHERE date >= "{start_date}" AND date <= "{end_date}"
            GROUP BY id1, id2
    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    edges = query_job.to_dataframe() 
    
    return(edges)

def get_nodes(location_id, start_date, end_date ):
    # Nodes

    query = f"""
            SELECT identifier
            FROM grafos-alcaldia-bogota.transits.hourly_transits
            WHERE location_id = "{location_id}"
                  AND date >= "{start_date}"
                  AND date <=  "{end_date}"
            GROUP BY identifier
    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    nodes = query_job.to_dataframe() 
    
    return(nodes)
    
    
def get_contacts(dataset_id, location_id, date, hour):
    '''
    Extracts the contacts of the given lo cation at date and hour
    '''
    
    # Edges 
    query = f"""
            SELECT id1, id2, lat, lon
            FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
            WHERE date = "{date}" AND hour = {hour}

    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    edges = query_job.to_dataframe() 
    
    return(edges)



def get_contacs_by_location(dataset_id, location_id, start_date, end_date):
    '''
    Extracts the contacts of the given lo cation at date and hour
    '''
    
    # Edges 
    query = f"""
            SELECT lat, lon, COUNT(*) total_contacts
            FROM
            (
              SELECT  ROUND(lat,4) AS lat, ROUND(lon,4) AS lon
              FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
              WHERE date >= "{start_date}" AND date <= "{end_date}"

            )
            GROUP BY lat, lon
            ORDER BY total_contacts DESC

    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    edges = query_job.to_dataframe() 
    
    return(edges)