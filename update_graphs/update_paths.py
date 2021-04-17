# Script that updates the paths
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

    client = bigquery.Client(location="US")

    update_start = time.time()

    # Today
    today = utils.get_today()


    # Extracts summary
    print('Extracts Coverage Summary for all Locations ')
    df_coverage = utils.get_path_coverage_of_depto_codes(client)

    # Filters out
    selected = df_coverage[(df_coverage.max_date.isna()) | (df_coverage.max_date + timedelta(days = 1) < today)]
    
    # Includes only active depto codes
    selected = selected[selected.code_depto.isin(utils.get_active_depto_codes(client))]
    
    print(f'Updating for {selected.shape[0]} codes' )
    print('')

    i = 0
    total_codes = selected.shape[0]
    for ind, row in selected.iterrows():

        # Counter
        i += 1

        start_date= utils.global_min_date
        if not pd.isna(row.max_date):
            start_date = pd.to_datetime(row.max_date) + timedelta(days = 1)

        # Today
        current_date = start_date
        end_date = today

        code_depto = row.code_depto

        print(f'   Excecuting: {code_depto} ({i} of {total_codes}) from {start_date} to {end_date}')
        
        while current_date < end_date:
            
            current_date_string = current_date.strftime('%Y-%m-%d')
            utils.add_paths_for_code_on_date(client, code_depto, current_date_string)
            
            # Updates
            current_date = current_date + timedelta(days = 1)
            
            print(f"      {current_date_string} OK")


        print(f'   Global Time Ellapsed: {np.round((time.time() - update_start)/60, 2)} minutes')
        print()

    print(f'Total time: {np.round((time.time() - update_start)/60, 2)} minutes')
    
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()