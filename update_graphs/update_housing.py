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
    
    today = pd.to_datetime(datetime.today())
    
    # Updates Houding
    print('Updates Housing')
    utils.update_housing_colombia(client, today.strftime(utils.date_format))
    print('Done')
    
    
if __name__ == "__main__":
    
    # Exceute Main
    main()