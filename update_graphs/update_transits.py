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

    # Gets the coverage
    client = bigquery.Client(location="US")


    # Transits
    df_transits = utils.get_transits_coverage(client)

    # Locations
    df_locations = utils.get_current_locations(client)

    # Merges
    df_locations = df_locations.merge(df_transits, on = 'location_id', how = 'left')
    
    
    today = utils.get_today()
    
    # Updates Bogota
    utils.update_bogota_sample(client, today.strftime(utils.date_format))

    # Filters out
    selected = df_locations[(df_locations.max_date.isna()) | (df_locations.max_date + timedelta(days = 1) < today)]

    start_time = time.time()
    print('Started')

    total_locations = selected.shape[0]
    i = 0
    for ind, row in selected.iterrows():
        # Counter
        i += 1

        # Start date
        start_date =  utils.global_min_date.strftime( utils.date_format)
        if not pd.isna(row.max_date):
            start_date = (pd.to_datetime(row.max_date) + timedelta(days = 1)).strftime(utils.date_format)

        # Today
        end_date = today.strftime( utils.date_format)

        # Location 
        location_id = row.location_id

        print(f'   Excecuting: {location_id} ({i} of {total_locations})')

        utils.compute_transits(client, location_id, start_date, end_date, ident = '      ')

        print(f'   Current Time: {np.round((time.time() - start_time)/60,2)} minutes')
        print()

    print()
    print('-------------------')
    print(f'Total Time: {np.round((time.time() - start_time)/60,2)} minutes')
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()