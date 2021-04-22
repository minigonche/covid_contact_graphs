# Script that updates the connections
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

    # Parameters
    jump = 12 # Hours
    treshold = 30 # Time Treshold Seconds
    accuracy = 30 # Start accuracy                    
    max_jump = 12 # Max jump (hours)
    max_accuracy = 30 # Max Accuracy
    accuracy_jump = 2 # The accuracy jump
    verbose = True


    # Extracts summary
    print('Extracts Coverage Summary for all Locations ')

    # Max date is inclusive
    df_coverage = utils.get_coverage_of_depto_codes(client)

    # df_coverage must be updated and saved at the end of this proedure to ensure consistency

    # If start date is missing, inserts global min_date
    df_coverage.loc[df_coverage.min_date.isna(), 'min_date'] = utils.global_min_date

    # Filters out
    selected = df_coverage[(df_coverage.max_date.isna()) | (df_coverage.max_date + timedelta(hours = 1) < today)]
    
    # Includes only active depto codes
    selected = selected[selected.code_depto.isin(utils.get_active_depto_codes(client))]
    

    print(f'Updating for {selected.shape[0]} codes' )
    print('')

    i = 0
    total_codes = selected.shape[0]
    for ind, row in selected.iterrows():

        # Counter
        i += 1

        start_time = utils.global_min_date
        if not pd.isna(row.max_date):
            start_time = pd.to_datetime(row.max_date) + timedelta(hours = 1)

        # Today
        end_time = pd.to_datetime(today.strftime('%Y-%m-%d 00:00:00'))

        code_depto = row.code_depto

        print(f'Excecuting: {code_depto} ({i} of {total_codes}) from {start_time} to {end_time}')


        utils.update_contacts_for_depto_code( client = client,
                                        code_depto = code_depto, 
                                        start_time = start_time, 
                                        end_time = end_time,
                                        jump = jump, 
                                        treshold = treshold, 
                                        accuracy = accuracy,               
                                        max_jump = max_jump, 
                                        max_accuracy = max_accuracy, 
                                        accuracy_jump = accuracy_jump, 
                                        verbose = verbose)

        print(f'Global Time Ellapsed: {np.round((time.time() - update_start)/60, 2)} minutes')
        print()

    print(f'Total time: {np.round((time.time() - update_start)/60, 2)} minutes')
    print(f'Saves Contacts Coverage')

    # Updates coverage
    # Max date must be inclusive
    df_coverage.loc[selected.index, 'max_date'] = today - timedelta(hours = 1)
    # Saves contact coverage
    utils.save_contacts_coverage(client, df_coverage)

        
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()