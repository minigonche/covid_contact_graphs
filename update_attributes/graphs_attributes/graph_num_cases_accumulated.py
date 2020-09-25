# Graph Number of cases attribute


from graph_attribute_generic import GenericGraphAttribute
import pandas as pd
import utils
import numpy as np
import positive_db_functions as pos_fun

attribute_name = 'number_of_cases_accumulated'




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

class GraphNumberOfCasesAccumulated(GenericGraphAttribute):
    '''
    Script that computes the number of cases by the given week
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, attribute_name)  


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
                - attribute_name (str): The attribute name                
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
           
        in_bogota = utils.is_in_bogota(self.client, graph_id, self.df_codes)
        in_palmira = utils.is_in_palmira(self.client, graph_id, self.df_codes)
        
        if in_bogota:
            query = bogota_sql.format(end_date_string = end_date_string, location_id = graph_id)
            
        elif in_palmira:
            query = generic_sql.format(table_name = 'palmira', end_date_string = end_date_string, location_id = graph_id)
            
        # Computes the total
        response = utils.run_simple_query(self.client, query)
        
        # Sets the value
        response.rename(columns = {'total':'value'}, inplace = True)
                
        # Adds the attribute name
        response['attribute_name'] = self.attribute_name
        

        # Returns the value
        return(response)
    
    
    
    

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
            