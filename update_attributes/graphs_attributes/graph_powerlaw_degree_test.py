# Excecutes a Power Law test over the degrees of the nodes


from graph_attribute_generic import GenericGraphAttribute
import pandas as pd
import numpy as np
import utils

attribute_name = 'powerlaw_degree_p_value'

# This Class has multiple attrbiute names



class GraphPowerLawTest(GenericGraphAttribute):
    '''
    Script that computes the gini index of the nodes pagerank.
    
    This script uses the results from the pagerank node attribute. If nothing is found will return None and writes a warning
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, attribute_name)
        
        # Extracts the locations
        self.df_locations = utils.get_current_locations(self.client)
        self.df_locations.index = self.df_locations.location_id        
                

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
            WHERE location_id = {graph_id} AND attribute_name = "node_degree" AND date = "{end_date_string}"
        """
        
        df = utils.run_simple_query(self.client, query)
        
        if df.shape[0] == 0:
            raise ValueError(f'No node degree found for {graph_id} on {end_date_string}')
        
        
        # Excecutes the power law test
        
        df_response = pd.DataFrame({'value':gini_inex, 'attribute_name':self.attribute_name })

        
        return(df_response)
    
    
