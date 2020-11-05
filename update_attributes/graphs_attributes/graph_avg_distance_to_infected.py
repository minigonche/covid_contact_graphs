# Average distance to infected


from graph_attribute_generic_with_cases import GenericGraphAttributeWithCases
import pandas as pd
import numpy as np
import utils
import positive_db_functions as pos_fun

# Dictionary to include property values
property_values = {}

# Attribute name
property_values['attribute_name'] = 'average_distance_to_infected'


class GraphAvgDistanceToInfected(GenericGraphAttributeWithCases):
    '''
    Script that computes the average distance to infected

    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttributeWithCases.__init__(self, property_values)
        
        

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
            SELECT AVG(attribute_value) as value
            FROM {utils.nodes_attribute_table}
            WHERE location_id = "{graph_id}" AND attribute_name = "distance_to_infected" AND date = "{end_date_string}"
        """
        
        df = utils.run_simple_query(self.client, query)
        
        # Extracts the value
        value = df['value'].values[0]
        
        if pd.isna(value):
            print('             ' + f'No distance to infected found for {graph_id} on {end_date_string}')
            return(pd.DataFrame({'value':None, 'attribute_name':[self.attribute_name] }))
        df_response = pd.DataFrame({'value':[value], 'attribute_name':[self.attribute_name] })
        
        return(df_response)