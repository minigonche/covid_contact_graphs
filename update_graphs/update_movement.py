# Updates movement
import utils
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import time
import numpy as np


def main():

    # Extracts the current date
    # Extracts the current date. Substract one day and the goes back to the colsest sunday
    today =utils.get_today()
  
    # Extracts the locations
    client = bigquery.Client(location="US")

    # All locations
    df_locations = utils.get_current_locations(client)
    
    # max dates
    df_locations_mov = utils.get_max_dates_for_graph_movement(client)
    
    df_locations = df_locations.merge(df_locations_mov, on = 'location_id', how = 'left')

    end_date = today
    
    
    # Filters out
    df_locations = df_locations[(df_locations.max_date.isna()) | (df_locations.max_date + timedelta(days = 1) < end_date)]

    print(f'Computing for {df_locations.shape[0]} Graphs')
    
    start_time = time.time()
    i = 0
    for ind, row in df_locations.iterrows():
        
        location_id = row.location_id
        
        i += 1
        print(f'   Computing { row.location_id}')

        if pd.isna(row.max_date):
            start_date = utils.global_min_date
        else:
            start_date = row.max_date + timedelta(days = 1) # Next day

        print(f'      Calculating for {row.location_id} ({i} of {df_locations.shape[0]}), from: {start_date} to {end_date}')

        current_date = start_date
        while current_date < end_date:
            
            date_string = current_date.strftime( utils.date_format)
            
            utils.compute_movement(client, location_id, date_string)
            
            print(f'         {current_date}: OK.')
            current_date = current_date + timedelta(days = 1)
            
        print(f'   Elapsed Time: {np.round((time.time() - start_time)/3600,3)} hours')


    print('')
    print('---------------------------------------')
    print('')

    print('All Done')



if __name__ == "__main__":
    
    # Exceute Main
    main()