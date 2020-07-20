# Update Attributes
# Script that computes all the attributes


# imports all the attributes
# Graphs
import  graphs_attributes.graph_size as graph_size
import utils
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
# Nodes



# Include here the desired graph attributes
# ------------------------------------
all_graph_attributes = []
all_graph_attributes.append(graph_size.GraphSize())



# Include here the desired node attributes
# ------------------------------------
all_node_attributes = []




def main():

    # Extracts the current date
    # Extracts the current date. Substract one day and the goes back to the colsest sunday
    end_date = pd.to_datetime(datetime.today()) - timedelta(days = 1)
    while end_date.dayofweek != 6:
        end_date = end_date - timedelta(days = 1)         

    # Extracts the locations
    client = bigquery.Client(location="US")
    df_locations_all = utils.get_current_locations(client)





    print(f'Computing {len(all_graph_attributes)} Graphs Attributes')
    # Excecutes all the graph attributes
    for g_att in all_graph_attributes:
        
        print(f'   Computing { g_att.attribute_name}.')
        df_att = utils.get_max_dates_for_graph_attribute(client, g_att.attribute_name)

        # Merges
        df_current = df_locations_all[['location_id']].merge(df_att, on = 'location_id', how = 'left')

        # Filters out
        selected = df_current[(df_current.max_date.isna()) | (df_current.max_date < end_date)]

        print(f'      Found {selected.shape[0]}')

        for ind, row in selected.iterrows():

            
            if pd.isna(row.max_date):
                start_date = g_att.starting_date
            else:
                start_date = row.max_date + timedelta(days = 7) # Next sunday

            print(f'         Calculating for {row.location_id}, from: {start_date.date()} to {end_date.date()}')

            current_date = start_date
            while current_date <= end_date:

                if g_att.location_id_supported(row.location_id, current_date):
                    year, week = utils.get_year_and_week_of_date(current_date)
                    g_att.save_attribute_for_week(row.location_id, year, week)
                    print(f'            {current_date.date()}: OK.')
                else:
                    print(f'            {current_date.date()}: Skipped by implementation.')

                current_date = current_date + timedelta(days = 7)


    print()
    print('Completed Graphs Attribute')
    print('---------------------------------------')
    print('')



    print(f'Computing {len(all_node_attributes)} Node Attributes')
    # Excecutes all the Node attributes
    for n_att in all_node_attributes:
        
        print(f'   Coputing { n_att.attribute_name}.')
        df_att = util.get_max_dates_for_node_attribute(client, n_att.attribute_name)

        # Merges
        df_current = df_locations_all[['location_id']],merge(df_att, on = 'location_id', how = 'left')

        # Filters out
        selected = df_current[(df_current.max_date.isna()) | (df_current.max_date < end_date)]

        print(f'      Found {selected.shape[0]}')

        for ind, row in selected.iterrows():

            
            if pd.isna(row.max_date):
                start_date = n_att.starting_date
            else:
                start_date = row.max_date + timedelta(days = 7) # Next sunday

            print(f'         Calculating for {row.location_id}, from: {start_date} to {end_date}')

            current_date = start_date
            while start_date <= end_date:

                if n_att.location_id_supported(row.location_id, current_date):
                    year, week = util.get_year_and_week_of_date(current_date)
                    n_att.save_attribute_for_week(row.location_id, year, week)
                    print(f'            {current_date}: OK.')
                else:
                    print(f'            {current_date}: Skipped by implementation.')

            current_date = current_date + timedelta(days = 7)


    print()
    print('Completed Node Attribute')
    print('---------------------------------------')
    print('')


    print('All Done')



if __name__ == "__main__":
    
    # Exceute Main
    main()