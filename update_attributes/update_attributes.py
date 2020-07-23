# Update Attributes
# Script that computes all the attributes
import utils
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd


# imports all the attributes

# Nodes
import nodes_attributes.node_degree as node_degree
import nodes_attributes.node_pagerank as node_pagerank



# Graphs
import graphs_attributes.graph_size as graph_size
import graphs_attributes.graph_num_contacts as graph_num_contacts
import graphs_attributes.graph_pagerank_gini as graph_pagerank_gini



# Include here the desired node attributes
# ------------------------------------
all_node_attributes = []
all_node_attributes.append(node_degree.NodeDegree())
all_node_attributes.append(node_pagerank.NodePageRank())



# Include here the desired graph attributes
# ------------------------------------
all_graph_attributes = []
all_graph_attributes.append(graph_size.GraphSize())
all_graph_attributes.append(graph_num_contacts.GraphNumberOfContacts())
all_graph_attributes.append(graph_pagerank_gini.GraphPageRankGini())



def main():

    # Extracts the current date
    # Extracts the current date. Substract one day and the goes back to the colsest sunday
    end_date = pd.to_datetime(datetime.today()) - timedelta(days = 1)
    while end_date.dayofweek != 6:
        end_date = end_date - timedelta(days = 1)         

    # Extracts the locations
    client = bigquery.Client(location="US")
    df_locations_all = utils.get_current_locations(client)
    
    # Nodes
    df_att_all = utils.get_max_dates_for_node_attributes(client)

    print(f'Computing {len(all_node_attributes)} Node Attributes')
    # Excecutes all the Node attributes
    for n_att in all_node_attributes:
        
        print(f'   Coputing { n_att.attribute_name}.')
        df_att = df_att_all[df_att_all.attribute_name == n_att.attribute_name].copy()

        # Merges
        df_current = df_locations_all[['location_id']].merge(df_att, on = 'location_id', how = 'left')
        
        # Removes the locations with no support
        df_current = df_current[df_current.location_id.apply(lambda l: n_att.location_id_supported(l))]

        # Filters out
        selected = df_current[(df_current.max_date.isna()) | (df_current.max_date < end_date)]

        print(f'      Found {selected.shape[0]}')
        
        i = 0
        for ind, row in selected.iterrows():
            
            i += 1

            if pd.isna(row.max_date):
                start_date = n_att.starting_date
            else:
                start_date = row.max_date + timedelta(days = 7) # Next sunday

            print(f'         Calculating for {row.location_id} ({i} of {selected.shape[0]}), from: {start_date} to {end_date}')

            current_date = start_date
            while current_date <= end_date:

                if n_att.location_id_supported_on_date(row.location_id, current_date):
                    year, week = utils.get_year_and_week_of_date(current_date)
                    n_att.save_attribute_for_week(row.location_id, year, week)
                    print(f'            {current_date}: OK.')
                else:
                    print(f'            {current_date}: Skipped by implementation.')

                current_date = current_date + timedelta(days = 7)


    print()
    print('Completed Node Attribute')
    print('---------------------------------------')
    print('')

    
    # Graphs
    df_att_all = utils.get_max_dates_for_graph_attributes(client)

    print(f'Computing {len(all_graph_attributes)} Graphs Attributes')
    # Excecutes all the graph attributes
    for g_att in all_graph_attributes:
        
        print(f'   Computing { g_att.attribute_name}.')
        df_att = df_att_all[df_att_all.attribute_name == g_att.attribute_name].copy()

        # Merges
        df_current = df_locations_all[['location_id']].merge(df_att, on = 'location_id', how = 'left')
        
        # Removes the locations with no support
        df_current = df_current[df_current.location_id.apply(lambda l: g_att.location_id_supported(l))]
        
        # Filters out
        selected = df_current[(df_current.max_date.isna()) | (df_current.max_date < end_date)]

        print(f'      Found {selected.shape[0]}')
        i = 0
        for ind, row in selected.iterrows():

            i += 1
            
            if pd.isna(row.max_date):
                start_date = g_att.starting_date
            else:
                start_date = pd.to_datetime(row.max_date) + timedelta(days = 7) # Next sunday

            print(f'         Calculating for {row.location_id} ({i} of {selected.shape[0]}), from: {start_date.date()} to {end_date.date()}')

            current_date = start_date
            while current_date <= end_date:

                if g_att.location_id_supported_on_date(row.location_id, current_date):
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

    print('All Done')



if __name__ == "__main__":
    
    # Exceute Main
    main()