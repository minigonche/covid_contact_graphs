# Update Attributes
# Script that computes all the attributes
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import pandas as pd
import time
import numpy as np
from datetime import timedelta, datetime
import os

# Custom Scripts
import utils


# imports all the attributes

# Nodes
import nodes_attributes.node_distance_to_infected as node_distance_to_infected
import nodes_attributes.node_degree as node_degree
import nodes_attributes.node_pagerank as node_pagerank
import nodes_attributes.node_betweenness as node_betweenness
import nodes_attributes.node_eigenvector as node_eigenvector
import nodes_attributes.node_personalized_pagerank as node_personalized_pagerank



# Graphs
import graphs_attributes.graph_size as graph_size
import graphs_attributes.graph_num_edges as graph_num_edges
import graphs_attributes.graph_num_contacts as graph_num_contacts
import graphs_attributes.graph_pagerank_gini as graph_pagerank_gini
import graphs_attributes.graph_betweenness_gini as graph_betweenness_gini
import graphs_attributes.graph_eigenvector_gini as graph_eigenvector_gini
import graphs_attributes.graph_personalized_pagerank_gini as graph_personalized_pagerank_gini
import graphs_attributes.graph_powerlaw_degree_test as graph_powerlaw_degree_test
import graphs_attributes.graph_eigenvalue_weighted as graph_eigenvalue_weighted
import graphs_attributes.graph_eigenvalue_unweighted as graph_eigenvalue_unweighted
import graphs_attributes.graph_transitivity as graph_transitivity
import graphs_attributes.graph_num_cases_accumulated as graph_num_cases_accumulated





# Include here the desired node attributes
# ------------------------------------
all_node_attributes = []
all_node_attributes.append(node_distance_to_infected.NodeDistanceToInfected())
all_node_attributes.append(node_degree.NodeDegree())
all_node_attributes.append(node_pagerank.NodePageRank())
all_node_attributes.append(node_eigenvector.NodeEigenvector())
all_node_attributes.append(node_personalized_pagerank.NodePersonalizedPageRank())
#all_node_attributes.append(node_betweenness.NodeBetweenness())



# Include here the desired graph attributes
# ------------------------------------
all_graph_attributes = []
all_graph_attributes.append(graph_size.GraphSize())
all_graph_attributes.append(graph_num_edges.GraphNumEdges())
all_graph_attributes.append(graph_num_contacts.GraphNumberOfContacts())
all_graph_attributes.append(graph_pagerank_gini.GraphPageRankGini())
all_graph_attributes.append(graph_eigenvector_gini.GraphEigenvectorGini())
all_graph_attributes.append(graph_personalized_pagerank_gini.GraphPersonalizedPageRankGini())
all_graph_attributes.append(graph_powerlaw_degree_test.GraphPowerLawTest())
all_graph_attributes.append(graph_eigenvalue_unweighted.GraphEigenValueUnweighted())
all_graph_attributes.append(graph_eigenvalue_weighted.GraphEigenValueWeighted())
all_graph_attributes.append(graph_transitivity.GraphTransitivity())
all_graph_attributes.append(graph_num_cases_accumulated.GraphNumberOfCasesAccumulated())

#all_graph_attributes.append(graph_betweenness_gini.GraphBetweennessGini())



def main():

    # Extracts the current date
    # Extracts the current date. Substract one day and the goes back to the closest sunday
    end_date = utils.get_today(only_date = True) - timedelta(days = 1)
    while end_date.dayofweek != 6:
        end_date = end_date - timedelta(days = 1)         

    # Extracts the locations
    client = bigquery.Client(location="US")
    df_locations_all = utils.get_current_locations(client)
    
    # Nodes
    df_att_all = utils.get_max_dates_for_node_attributes(client)

    print(f'Computing {len(all_node_attributes)} Node Attributes')
    # Excecutes all the Node attributes
    
    start_time = time.time()
    
    j = 0
    for n_att in all_node_attributes:
        
        j += 1
        print(f'   Coputing { n_att.attribute_name}. ({j} of {len(all_node_attributes)})')
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
            
            print(f'   Elapsed Time: {np.round((time.time() - start_time)/3600,3)} hours')


    print()
    print('Completed Node Attribute')
    print('---------------------------------------')
    print('')

    
    # Graphs
    df_att_all = utils.get_max_dates_for_graph_attributes(client)

    print(f'Computing {len(all_graph_attributes)} Graphs Attributes')
    # Excecutes all the graph attributes
    
    j = 0
    for g_att in all_graph_attributes:
        
        j += 1
        print(f'   Computing { g_att.attribute_name} ({j} of {len(all_graph_attributes)})')
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
            
            print(f'   Elapsed Time: {np.round((time.time() - start_time)/3600,3)} hours')
            

    print()
    print('Completed Graphs Attribute')
    print('---------------------------------------')
    print('')

    print('All Done')



if __name__ == "__main__":
    
    # Exceute Main
    main()