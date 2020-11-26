# Graph Number of cases attribute


from graph_attribute_generic_with_cases import GenericGraphAttributeWithCases
import pandas as pd
import utils
import numpy as np
import positive_db_functions as pos_fun


# Dictionary to include property values
property_values = {}

# Attribute name
property_values['attribute_name'] = 'number_of_cases_accumulated'



# Queries
# ---------
# Generic Query
generic_sql = """

        SELECT COUNT(*) as total
        FROM  grafos-alcaldia-bogota.positives.{table_name}
        WHERE  date_start_symtoms <= "{end_date_string}"
            AND ST_DWithin(ST_GeogPoint(lon, lat), 
                (SELECT geometry FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"), 
                  (SELECT precision FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"))
"""


# Bogota
# --------
bogota_sql = """

        SELECT COUNT(*) as total
        FROM  `servinf-unacast-prod.AlcaldiaBogota.positivos_agg_fecha` 
        WHERE  fechainici <= "{end_date_string}"
           AND ST_DWithin(geometry, 
                (SELECT geometry FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"), 
                  (SELECT precision FROM grafos-alcaldia-bogota.geo.locations_geometries WHERE location_id = "{location_id}"))
"""

class GraphNumberOfCasesAccumulated(GenericGraphAttributeWithCases):
    '''
    Script that computes the number of cases by the given week
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
                - attribute_name (str): The attribute name                
                - value (float): The value of the attribute
        '''
    
        raise ValueError('Should not enter here')
    
    
    def compute_attribute_for_interval(self, location_id, start_date_string, end_date_string):
        '''
        Method that computes the attribute of the class for the given dates. Edit this method if the attributes requieres more than just the nodes and
        the ids. See weighted_pagerank for an example.

        parameters
            - location_id(str): The graph id
            - start_date_string (str): Start date in %Y-%m-%d
            - end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the structure of the output of the method compute_attribute   
        '''
           
        city = utils.get_city(self.client, location_id, self.df_codes)
                
        if city == utils.BOGOTA:
            query = bogota_sql.format(end_date_string = end_date_string, location_id = location_id)
            
        else:
            query = generic_sql.format(table_name = city, end_date_string = end_date_string, location_id = location_id)
            
        # Computes the total
        response = utils.run_simple_query(self.client, query)
        
        # Sets the value
        response.rename(columns = {'total':'value'}, inplace = True)
                
        # Adds the attribute name
        response['attribute_name'] = self.attribute_name
        

        # Returns the value
        return(response)
    
    
            