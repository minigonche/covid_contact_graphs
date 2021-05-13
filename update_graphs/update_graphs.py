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
    
    today = utils.get_today()

    
    print('   Extracts Coverage')
    df_locations = utils.get_edgelists_coverage(client)

    # Filters out
    selected = df_locations[(df_locations.end_date.isna()) | (df_locations.end_date + timedelta(days = 1) < today)] # Substracts one day so that it does not compute partial days
    
        
    # Min Support Date
    df_min_support_date = utils.get_min_support_date_for_location_attributes(client)
    df_min_support_date.index = df_min_support_date.location_id    
    
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
        
        # Checks if dataset exists
        if not row.dataset_exists:
            print(f"      Dataset {row.dataset} not found, creating dataset...")
            utils.create_dataset(client, row.dataset)
            
        
        # Checks if the table exists (if the end_date is null)
        if not row.table_exists:
            print(f"      No table found for {location_id}, creating table...")
            utils.create_edgelist_table(client, row.dataset, row.location_id)
            
        
        # Start date
        curent_date = utils.global_min_date.date()
        
        # Checks for min support
        if location_id in df_min_support_date.index:
            curent_date = pd.to_datetime(df_min_support_date.loc[location_id,'min_date'])
            
        if not pd.isna(row.end_date):
            curent_date = (pd.to_datetime(row.end_date) + timedelta(days = 1))
        
        final_date = today.date()

        while curent_date < final_date: # Does not include last day

            print(f"      Computing {curent_date}")

            utils.add_edglists_to_graph(client, row.dataset, location_id, curent_date.strftime( utils.date_format))

            #updates
            curent_date = curent_date + timedelta(days = 1)

        print(f'   Current Time: {np.round((time.time() - start_time)/60,2)} minutes')
        print()
        
        df_locations.loc[ind, 'end_date'] = final_date - timedelta(days = 1)
    
    
    # Sends the dates back 10 days for safety
    for ind, row in df_locations.iterrows():        
        #updates the value
        df_locations.loc[ind, 'end_date'] = df_locations.loc[ind, 'end_date'] - timedelta(days = 10) # Sets back 10 days, to avoid this script to run with transit compution failing



    print('Saves coverage')
    utils.refresh_graphs_coverage(client, df_locations)
    print()
    print('-------------------')
    print(f'Total Time: {np.round((time.time() - start_time)/60,2)} minutes')
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()