# Loads the different libraries
import pandas as pd
import numpy as np
from google.cloud import bigquery
import igraph as ig
# Starts the client
client = bigquery.Client(location="US")
import seaborn as sns
from datetime import timedelta

import matplotlib.pyplot as plt

import geopandas
import contextily as ctx

date_format = "%Y-%m-%d"

def compute_personalized_pagerank(nodes, edges, weighted):
    '''
    Computes personalized pagerank. Nodes must have weight attribute
    '''
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
    
    if weighted:
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the personalized pagerank
        personalized_page_rank = G.personalized_pagerank(weights = 'weight', directed = False, reset = nodes['dist_weight'].values)
    
    else:
        personalized_page_rank = G.personalized_pagerank(directed = False, reset = nodes['dist_weight'].values)
        
    # Returns the value
    return(personalized_page_rank)


def compute_pagerank(nodes, edges, weighted):
    
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
    
    if weighted:
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the personalized pagerank
        page_rank = G.pagerank(weights = 'weight', directed = False)
    
    else:
        page_rank = G.pagerank(directed = False)
        
    # Returns the value
    return(page_rank)    


    
def compute_eigenvector(nodes, edges, weighted):
    '''
    Computes eigen vector
    '''
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
    
    if weighted:
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the personalized pagerank
        eigen_vector = G.evcent(weights = 'weight', directed = False)
    
    else:
        # Exctracs the personalized pagerank
        eigen_vector = G.evcent(directed = False)
    
    # Returns the value
    return(eigen_vector)    
    
    
def compute_eigenvalue(nodes, edges, weighted):
    '''
    Computes personalized pagerank.
    '''
    
    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        

    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))

    if weighted:        
        # Adds weights to edges
        G.es['weight'] = edges.weight.values

        # Exctracs the eigen value
        _, eigen_value = G.evcent(weights = 'weight', directed = False, return_eigenvalue = True)

    else:

        # Exctracs the eigen value
        _, eigen_value = G.evcent(directed = False, return_eigenvalue = True)
        
    # Returns the value
    return(eigen_value)


def sort_edges_by_centrality(nodes, edges, centrality):
    
    df_centrality = pd.DataFrame({'val':centrality}, index = nodes.identifier)
    
    new_edges = edges.copy()
    new_edges['impact'] = df_centrality.loc[new_edges.id1, 'val'].values*df_centrality.loc[new_edges.id2, 'val'].values
    new_edges.sort_values('impact', ascending = False, inplace = True)
    
    return(new_edges)


def sort_edges(nodes, edges, method, weighted = False):
    '''
    Sorts the edges
    
    '''
    
    if method.upper() == 'RANDOM':
        return(edges.sample(edges.shape[0]))
    
    if method.upper() == 'EIGENVECTOR':
        
        centrality = compute_eigenvector(nodes, edges, weighted)
        return(sort_edges_by_centrality(nodes, edges, centrality))
    
    if method.upper() == 'PAGERANK':
        
        centrality = compute_pagerank(nodes, edges, weighted)
        return(sort_edges_by_centrality(nodes, edges, centrality))
    
    
    raise ValueError(f'Unsupported Method: {method}')
    
    
def get_edges(dataset_id, location_id, start_date, end_date ):
    
    
    # Edges 
    query = f"""
            SELECT id1, id2, COUNT(*) as weight, SUM(contacts) as total_contacts
            FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
            WHERE date >= "{start_date}" AND date <= "{end_date}"
            GROUP BY id1, id2
    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    edges = query_job.to_dataframe() 
    
    return(edges)

def get_nodes(location_id, start_date, end_date ):
    # Nodes

    query = f"""
            SELECT identifier
            FROM grafos-alcaldia-bogota.transits.hourly_transits
            WHERE location_id = "{location_id}"
                  AND date >= "{start_date}"
                  AND date <=  "{end_date}"
            GROUP BY identifier
    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    nodes = query_job.to_dataframe() 
    
    return(nodes)
    
    
def get_contacts(dataset_id, location_id, date, hour):
    '''
    Extracts the contacts of the given lo cation at date and hour
    '''
    
    # Edges 
    query = f"""
            SELECT id1, id2, lat, lon
            FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
            WHERE date = "{date}" AND hour = {hour}

    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    edges = query_job.to_dataframe() 
    
    return(edges)



def get_contacs_by_location(dataset_id, location_id, start_date, end_date):
    '''
    Extracts the contacts of the given lo cation at date and hour
    '''
    
    # Edges 
    query = f"""
            SELECT lat, lon, COUNT(*) total_contacts
            FROM
            (
              SELECT  ROUND(lat,4) AS lat, ROUND(lon,4) AS lon
              FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
              WHERE date >= "{start_date}" AND date <= "{end_date}"

            )
            GROUP BY lat, lon
            ORDER BY total_contacts DESC

    """


    job_config = bigquery.QueryJobConfig(allow_large_results = True)
    query_job = client.query(query, job_config=job_config) 

    # Return the results as a pandas DataFrame
    edges = query_job.to_dataframe() 
    
    return(edges)
    