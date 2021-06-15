# Update Statistics (weekñy number of nodes and edges)
# Script that computes all the attributes
import utils
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import time
import numpy as np


def main():

    # Extracts the current date. 
    end_date = utils.get_today() - timedelta(days = 1)
     
    # Extracts the locations
    client = bigquery.Client(location="US")
    df_locations = utils.get_current_locations(client)
    
    # Min Support Dates
    df_min_support_date = utils.get_min_support_date_for_location_attributes(client)
    df_min_support_date.index = df_min_support_date.location_id      
    
    # max dates
    df_dates = utils.get_max_dates_for_graph_statistics(client)
        
    # Merges
    df_locations = df_locations.merge(df_dates, on = 'location_id', how = 'left')
    

    # Filters out
    df_locations = df_locations[(df_locations.max_date.isna()) | (df_locations.max_date < end_date)]

    
    print(f'Computing for {df_locations.shape[0]} Graphs')
    
    start_time = time.time()
    i = 0
    for ind, row in df_locations.iterrows():
        
        location_id = row.location_id
        dataset_id = row.dataset
        
        i += 1
        print(f'   Computing { row.location_id}')
        
        
        start_date = utils.global_min_date
        
        # Checks for min support
        if location_id in df_min_support_date.index:
            start_date = pd.to_datetime(df_min_support_date.loc[location_id,'min_date'])
            
        if not pd.isna(row.max_date):
            start_date = row.max_date + timedelta(days = 1) # Next Day
        

        print(f'      Calculating for {row.location_id} ({i} of {df_locations.shape[0]}), from: {start_date} to {end_date}')
        
        current_date = start_date
        while current_date <= end_date:
            
            date_string = current_date.strftime( utils.date_format)
            
            sql = f"""
            INSERT INTO grafos-alcaldia-bogota.graph_attributes.graph_sizes (location_id, date,num_nodes, num_edges) 
            SELECT "{location_id}" as location_id,
                    DATE("{date_string}") as date,       
                    (SELECT COUNT(*) as num_nodes
                    FROM
                    (SELECT identifier
                    FROM grafos-alcaldia-bogota.transits.hourly_transits
                    WHERE location_id = "{location_id}"
                          AND date <= "{date_string}"
                          AND date >= DATE_SUB("{date_string}", INTERVAL {utils.global_attribute_window - 1} DAY) 
                    GROUP BY identifier )) as num_nodes,
                    (SELECT COUNT(*) as num_edges
                      FROM
                      (
                          SELECT id1, id2
                          FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
                          WHERE date <= "{date_string}"
                                AND date >= DATE_SUB("{date_string}", INTERVAL {utils.global_attribute_window - 1} DAY) 
                          GROUP BY id1, id2
                      )) as num_edges
            
            
            """
            
            job_config= bigquery.QueryJobConfig()
            query_job = client.query(sql, job_config= job_config) 
            query_job.result()

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