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
    start_time = time.time()
    client = bigquery.Client(location="US")
    
    today = utils.get_today()
    
    last_sunday = today
    while last_sunday.dayofweek != 6:
        last_sunday -= timedelta(days = 1)
    
    
    # Gets housing code coverage
    df_codes = utils.get_housing_code_coverage(client)
    
    # Only stays with colombia
    df_codes = df_codes[df_codes.code_depto.apply(lambda c: c.startswith('CO.'))]
    
    print('Updates Housing')
    
    selected = df_codes[(df_codes.max_date.isna()) | (df_codes.max_date < last_sunday)].copy()
    
    # Includes only active depto codes
    selected = selected[selected.code_depto.isin(utils.get_active_depto_codes(client))]    
    

    print(f'   Updates for {selected.shape[0]} codes')
    
    i = 0
    for ind, row in selected.iterrows():
        
        i += 1
        code_depto = row.code_depto
        start_date = pd.to_datetime(row.max_date)
        
        if pd.isna(start_date):
            start_date = utils.global_min_housing_sunday
        
        # Takes the sunday into monday
        start_monday = start_date
        while start_monday.dayofweek != 0:
            start_monday += timedelta(days = 1)
            
        if start_monday > last_sunday:
            raise ValueError(f'Something went wrong, monday should be behind sunday. Monday: {start_monday}, Sunday: {last_sunday}')
            
        monday_str = start_monday.strftime(utils.date_format)
        sunday_str = last_sunday.strftime(utils.date_format)
        print(f'      Updates for {code_depto} from {monday_str} to {sunday_str} ({i} of {selected.shape[0]})')
        
        utils.update_housing(client, code_depto, monday_str, sunday_str)
    
    print('Done')
    print(f'Total Time: {np.round((time.time() - start_time)/60,2)} minutes')
    print()
    print()
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()