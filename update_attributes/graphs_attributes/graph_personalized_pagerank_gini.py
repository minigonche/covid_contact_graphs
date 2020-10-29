# Gini Index over the personalized pagerank values of the nodes


from graph_attribute_generic import GenericGraphAttribute
import pandas as pd
import numpy as np
import utils
import positive_db_functions as pos_fun


# Dictionary to include property values
property_values = {}

# Attribute name
property_values['attribute_name'] = 'personalized_pagerank_gini_index'




class GraphPersonalizedPageRankGini(GenericGraphAttribute):
    '''
    Script that computes the gini index of the nodes personalized pagerank.
    
    This script uses the results from the pagerank node attribute. If nothing is found will return None and writes a warning
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, property_values)
        
        self.df_codes =  utils.get_geo_codes(self.client, location_id = None)
        self.df_codes.index = self.df_codes.location_id
        
        # Gets the max date of symptoms for each supported location        
        self.max_dates = pos_fun.get_positive_max_dates(self.client)               

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
            FROM {utils.nodes_attribute_table}
            WHERE location_id = "{graph_id}" AND attribute_name = "personalized_pagerank_centrality" AND date = "{end_date_string}"
        """
        
        df = utils.run_simple_query(self.client, query)
        
        if df.shape[0] == 0:
            print('             ' + f'No Personalized Pagerank Centrality found for {graph_id} on {end_date_string}')
            return(pd.DataFrame({'value':None, 'attribute_name':[self.attribute_name] }))
        
        
        # Computes the Gini Index
        gini_inex = utils.gini(df.attribute_value.values)
        
        df_response = pd.DataFrame({'value':[gini_inex], 'attribute_name':[self.attribute_name] })

        
        return(df_response)
    
    
    
    
    def location_id_supported(self, location_id):
        '''
        OVERWRITTEN
        # ---------------
        
        Method that determines if the attribute is supported for the location_id (graph).
        The default implementation is to return True.

        Overwrite this method in case the attribute is not on any date for a given location.
    
        NOTE: This method is called several times inside a loop. Make sure you don't acces any expensive resources in the implementation.
        
        params
            - location_id (str)
            - current_date (pd.datetime): the current datetime

        returns
            Boolean
        '''

        if location_id == 'colombia_university_rosario_campus_norte':
            return(False)
        
        return( pos_fun.has_positives_database(self.client, location_id, self.df_codes))
    

        
    def location_id_supported_on_date(self, location_id, current_date):
        '''
        OVERWRITTEN
        # --------------
        
        Method that determines if the attribute is supported for the location_id (graph) on a specific date
        The default implementation is to return True if the current date is equal or larger that the starting_date and is not inside hell week
        Overwrite this method in case the attribute is not supported for a certain location_id (or several) at a particular date
    
        NOTE: This method is called several times inside a loop. Make sure you don't acces any expensive resources in the implementation.
        
        params
            - location_id (str)
            - current_date (pd.datetime): the current datetime

        returns
            Boolean
        '''
        
        up_to_date = pos_fun.positives_up_to_date(self.client, location_id, current_date, self.df_codes, self.max_dates)
        
        return(up_to_date)  
    