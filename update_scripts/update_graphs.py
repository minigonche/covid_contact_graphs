from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import pandas as pd
import time
import numpy as np
from datetime import timedelta, datetime
import os

# Custom Scripts
import utils
        
        
def main():
    
    # Starts Client
    client = bigquery.Client(location="US")
    
    dataset_id = 'grafos-alcaldia-bogota.graphs'
    dataset = client.get_dataset(dataset_id)  # Make an API request.

    # Gets the tables
    tables = list(client.list_tables(dataset))  # Make an API request(s).

    df_locations = utils.get_current_locations(client)

    # Loads old coverage
    df_old_coverage = utils.get_last_graph_coverage(client)

    if df_old_coverage is not None:
        # merges
        df_locations = df_locations.merge(df_old_coverage, on = 'location_id', how = 'left')
    else:
        df_locations['start_date'] = None
        df_locations['end_date'] = None

    # Sets index (for updating)
    df_locations.index = df_locations.location_id


    print('Extracting Coverage')

    #Gets last coverage


    # Updates all the locations
    for table in tables:
        # Extracts the table id
        graph_name = table.table_id
        if graph_name in df_locations.location_id:        
            max_date = utils.get_date_range_for_graph_table(client, graph_name, df_locations.loc[graph_name,'end_date'])
            df_locations.loc[graph_name,'end_date'] = max_date
            
            if max_date is not None and  df_locations.loc[graph_name,'start_date'] is None:
                df_locations.loc[graph_name,'start_date'] =  utils.global_min_date

    # Iterates over the missing places 

    today = pd.to_datetime(datetime.today())

    #DEBUG
    #today = pd.to_datetime("2020-07-10")

    # Filters out
    selected = df_locations[(df_locations.end_date.isna()) | (df_locations.end_date + timedelta(days = 1) < today)]


    start_time = time.time()
    print('Started')


    if selected.shape[0] == 0:
        print('   No places found.')

    total_locations = selected.shape[0]
    i = 0
    for ind, row in selected.iterrows():
        # Counter
        i += 1

        # Location 
        location_id = row.location_id

        print(f'   Excecuting: {location_id} ({i} of {total_locations})')

        # Checks if the table exists (if the end_date is null)
        if row.start_date is None:
            print(f"      No table found for {location_id}, creating table...")
            utils.add_graph_table(client, location_id)
            df_locations.loc[ind, 'start_date'] = utils.global_min_date

        curent_date = utils.global_min_date
        if row.end_date is not None:
            curent_date = (pd.to_datetime(row.end_date) + timedelta(days = 1))

        final_date = today - timedelta(days = 1)

        while curent_date < final_date:

            print(f"      Computing {curent_date}")

            utils.add_edglists_to_graph(client, location_id, curent_date.strftime( utils.date_format))

            #updates
            curent_date = curent_date + timedelta(days = 1)

        print(f'   Current Time: {np.round((time.time() - start_time)/60,2)} minutes')
        print()

        #updates the value
        df_locations.loc[ind, 'end_date'] = final_date - timedelta(days = 1) # Sets back 15 days, to avoid this script to run with transit failing

    print('Saves coverage')
    utils.refresh_graphs_coverage(client, df_locations)
    print()
    print('-------------------')
    print(f'Total Time: {np.round((time.time() - start_time)/60,2)} minutes')
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()