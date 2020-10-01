# Updates the depto codes
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
    
    # Gets the current locations
    df_locations =  utils.get_current_locations(client)
    
    # Gets the ones with geo_codes
    df_codes = utils.get_locations_with_geo_codes(client)
    
    df_locations = df_locations[~df_locations.location_id.isin(df_codes.location_id)]
    
    if df_locations.shape[0] == 0:
        print('Depto Codes up to date.')
    
    else:
        print('Found {} New Locations'.format(df_locations.shape[0]))
        for ind, row in df_locations.iterrows():
            print(f'   Updating: {row.location_id}')
            utils.add_code_deptos(client, row.location_id)
            
    print('Done')
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()