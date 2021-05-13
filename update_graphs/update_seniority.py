# Script that updates seniority per depto code
# Seniority is how long a single identifier cas been inside the pipeline

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
    print('Extracts Seniority Coverage Summary for all Locations ')

    # Max date is inclusive
    df_coverage = utils.get_depto_code_seniority_coverage(client)

    # Filters out
    selected = df_coverage[(df_coverage.max_date.isna()) | (df_coverage.max_date + timedelta(days = 1) < end_date)]
    
        
    # Includes only active depto codes
    selected = selected[selected.code_depto.isin(utils.get_active_depto_codes(client))]
    

    print(f'Updating for {selected.shape[0]} codes' )
    print('')

    i = 0
    total_codes = selected.shape[0]
    for ind, row in selected.iterrows():

        # Counter
        i += 1

        start_time = utils.global_min_seniority_search
        if not pd.isna(row.max_date):
            start_time = pd.to_datetime(row.max_date) + timedelta(days = 1)
            

        # Today
        end_time = pd.to_datetime(today.strftime('%Y-%m-%d 00:00:00'))

        code_depto = row.code_depto

        print(f'Excecuting: {code_depto} ({i} of {total_codes}) from {start_time} to {end_time}')

        utils.update_seniority(client, code_depto, start_time, end_time)

        print(f'Global Time Ellapsed: {np.round((time.time() - update_start)/60, 2)} minutes')
        print()

    print(f'Total time: {np.round((time.time() - update_start)/60, 2)} minutes')

            
    
if __name__ == "__main__":
    
    # Exceute Main
    main()