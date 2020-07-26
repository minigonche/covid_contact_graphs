# Gini Index over the eigenvector values of the nodes


from graph_attribute_generic import GenericGraphAttribute
import pandas as pd
import numpy as np
import utils

attribute_name = 'eigenvector_gini_index'


# The gini index function
# Source: from https://github.com/oliviaguest/gini

def gini(array):
    """Calculate the Gini coefficient of a numpy array."""
    # Method copied from https://github.com/oliviaguest/gini
    
    array = np.array(array).flatten() #all values are treated equally, arrays must be 1d
    if np.amin(array) < 0:
        array -= np.amin(array) #values cannot be negative
    array = array + 0.0000001 #values cannot be 0
    array = np.sort(array) #values must be sorted
    index = np.arange(1,array.shape[0]+1) #index per array element
    n = array.shape[0]#number of array elements
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array))) #Gini coefficient


class GraphEigenvectorGini(GenericGraphAttribute):
    '''
    Script that computes the gini index of the nodes pagerank.
    
    This script uses the results from the pagerank node attribute. If nothing is found will return None and writes a warning
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, attribute_name)
                        

    def compute_attribute(self, nodes, edges):
        '''
        Main Method to Implement
        
        This method must be implemented by the subclass. It receives compact nodes and edges and 
        must output the corresponding attribute. This method must return unique identifiers and it's good 
        practice to include all identifiers. In case its a graph attribute the dataframe must contain only one row and the
        identifier column is ignored

        params
            nodes (pd.DataFrame) Pandas Dataframe with the nodes of the graph:
                - identifier (str): Id of the node
                - weight (int): Weight of the node (see get_compact_nodes) 
            edges (pd.DataFrame) Pandas dataFrame with the grouped edglist (undirected)
                - id1 (str) 
                - id2 (str)
                - weight (num) Weight od the edge (see get_compact_edgelist)      

        
        returns
            pd.DataFrame with the following structure
                - attribute_name (str): The attribute nam            
                - value (float): The value of the attribute
        '''
    
        raise ValueError('Should not enter here')
    
    
    
    def compute_attribute_for_interval(self, graph_id, start_date_string, end_date_string):
        '''
        Method that computes the attribute of the class for the given dates. Edit this method if the attributes requieres more than just the nodes and
        the ids. See weighted_pagerank for an example.

        parameters
            - graph_id(str): The graph id
            - start_date_string (str): Start date in %Y-%m-%d
            - end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the structure of the output of the method compute_attribute   
        '''
                
        query = f"""
            SELECT location_id, identifier, attribute_name, attribute_value
            FROM grafos-alcaldia-bogota.graph_attributes.node_attributes
            WHERE location_id = "{graph_id}" AND attribute_name = "eigenvector_centrality" AND date = "{end_date_string}"
        """
        
        df = utils.run_simple_query(self.client, query)

        if df.shape[0] == 0:
            message = f'No Betweenness Centrality found for {graph_id} on {end_date_string}'
            if graph_id == 'colombia_university_rosario_campus_norte':
                print('             ' + message)
                return(pd.DataFrame({'value':None, 'attribute_name':[self.attribute_name] }))
            else:                    
                raise ValueError(message)
                
        
        # Computes the Gini Index
        gini_inex = gini(df.attribute_value.values)
        
        df_response = pd.DataFrame({'value':[gini_inex], 'attribute_name':[self.attribute_name] })

        
        return(df_response)
    
    

    
    def location_id_supported_on_date(self, location_id, current_date):
        '''
        Method that determines if the attribute is supported for the location_id (graph) on a specific date
        The default implementation is to return True if the current date is equal or larger that the starting_date.
        Overwrite this method in case the attribute is not supported for a certain location_id (or several) at a particular date
    
        NOTE: This method is called several times inside a loop. Make sure you don't acces any expensive resources in the implementation.
        
        params
            - location_id (str)
            - current_date (pd.datetime): the current datetime

        returns
            Boolean
        '''
        
        # Has support for everything except hell week
        if current_date >= utils.hell_week[0] and current_date <= utils.hell_week[1]:
            return(False)
        
        # For medellin also include week 2
        if location_id == 'colombia_medellin' and current_date >= utils.hell_week_2[0] and current_date <= utils.hell_week_2[1]:
            return(False)   
        
        # For valle_del_cauca also include week 2
        if location_id == 'colombia_valle_del_cauca' and current_date >= utils.hell_week_2[0] and current_date <= utils.hell_week_2[1]:
            return(False)           
        
        return(True)    