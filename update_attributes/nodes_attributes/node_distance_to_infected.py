# Node distance to infected


from node_attribute_generic import GenericNodeAttribute
import pandas as pd
import numpy as np
import igraph as ig
import utils
import positive_db_functions as pos_fun

attribute_name = 'distance_to_infected'
priority = 2

# Queries
# ---------
# Generic Query
generic_sql = """

        with 
        -- Graph Ids
        graph_ids as (
             SELECT identifier
             FROM grafos-alcaldia-bogota.transits.hourly_transits
             WHERE location_id = "{graph_id}"
                   AND date <= "{end_date_string}"
                   AND date >  DATE_SUB(DATE("{end_date_string}"), INTERVAL 7 DAY) -- Los transitos de la ultima semana
             GROUP BY identifier
        ),
        -- Housing
        houses as (
          SELECT loc.identifier, lat,lon
          FROM grafos-alcaldia-bogota.housing_location.colombia_housing_location as loc
          JOIN graph_ids
          ON loc.identifier = graph_ids.identifier
          WHERE loc.week_date >= DATE_SUB(DATE("{end_date_string}"), INTERVAL 4 WEEK)
             AND loc.week_date <= DATE_ADD(DATE("{end_date_string}"), INTERVAL 1 WEEK)  -- Casas del ultimo mes (mas una semana)
           ),

        -- Distance to infected
        distances as (
        SELECT identifier, MIN(distance) as distance_to_infected
        FROM
        (
         SELECT
         houses.identifier as identifier,
         ST_DISTANCE(ST_GEOGPOINT(houses.lon, houses.lat), ST_GEOGPOINT(infectados.lon, infectados.lat)) as distance -- Distancia a infectado (en metros)
         FROM houses as houses -- Tabla con las casas
         CROSS JOIN (SELECT * FROM  positives.{table_name}
          WHERE  "{end_date_string}" >= DATE_SUB(DATE(date_start_symtoms), INTERVAL 5 DAY) 
                AND "{end_date_string}" <= DATE_ADD(DATE(date_start_symtoms), INTERVAL 15 DAY)   
          ) as infectados -- Tabla con los infectados (del ultimo mes)
         ) as d -- Matriz de distancia entre las casas y los infectados
         GROUP BY identifier -- Agrupa por el identificador para encontrar la minima distancia
         )


         -- Final Query
         SELECT graph_ids.identifier as identifier, distances.distance_to_infected as distance_to_infected
         FROM graph_ids
         LEFT JOIN distances
         ON graph_ids.identifier = distances.identifier

"""


# Bogota
# --------
bogota_sql = """
                
                with 
                -- Graph Ids
                graph_ids as (
                     SELECT identifier
                     FROM grafos-alcaldia-bogota.transits.hourly_transits
                     WHERE location_id = "{graph_id}"
                           AND date <= "{end_date_string}"
                           AND date >  DATE_SUB(DATE("{end_date_string}"), INTERVAL 7 DAY) -- Los transitos de la ultima semana
                     GROUP BY identifier
                ),
                -- Housing
                houses as (
                  SELECT loc.identifier, lat,lon
                  FROM grafos-alcaldia-bogota.housing_location.colombia_housing_location as loc
                  JOIN graph_ids
                  ON loc.identifier = graph_ids.identifier
                  WHERE loc.week_date >= DATE_SUB(DATE("{end_date_string}"), INTERVAL 4 WEEK)
                     AND loc.week_date <= DATE_ADD(DATE("{end_date_string}"), INTERVAL 1 WEEK)  -- Casas del ultimo mes (mas una semana)
                   ),

                -- Distance to infected
                distances as (
                SELECT identifier, MIN(distance) as distance_to_infected
                FROM
                (
                 SELECT
                 houses.identifier as identifier,
                 ST_DISTANCE(ST_GEOGPOINT(houses.lon, houses.lat), infectados.geometry) as distance -- Distancia a infectado (en metros)
                 FROM houses as houses -- Tabla con las casas
                 CROSS JOIN (SELECT * FROM `servinf-unacast-prod.AlcaldiaBogota.positivos_agg_fecha`                  
                    WHERE  TRIM(fechainici) <> "" AND "{end_date_string}" >= DATE_SUB(DATE(TIMESTAMP(fechainici)), INTERVAL 5 DAY) 
                            AND "{end_date_string}" <= DATE_ADD(DATE(TIMESTAMP(fechainici)), INTERVAL 15 DAY)   
                      ) as infectados -- Tabla con los infectados (del ultimo mes)             
                 ) as d -- Matriz de distancia entre las casas y los infectados
                 GROUP BY identifier -- Agrupa por el identificador para encontrar la minima distancia
                 )

                 -- Final Query
                 SELECT graph_ids.identifier as identifier, distances.distance_to_infected as distance_to_infected
                 FROM graph_ids
                 LEFT JOIN distances
                 ON graph_ids.identifier = distances.identifier                
                
        """



class NodeDistanceToInfected(GenericNodeAttribute):
    '''
    Script that computes the distance to infected
    '''

    def __init__(self):
        # Initilizes the super class
        GenericNodeAttribute.__init__(self, attribute_name = attribute_name, priority = priority)
            
        self.df_codes =  utils.get_geo_codes(self.client, location_id = None)
        self.df_codes.index = self.df_codes.location_id
        
        # Gets the max date of symptoms for each supported location        
        self.max_dates = pos_fun.get_positive_max_dates(self.client)
        
    

    # --- Global Abstract Methods
    def compute_attribute(self, nodes, edges):
        '''
        # TODO
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
                - identifier (str): Identifier of the node or graph
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
            query = bogota_sql.format(graph_id = graph_id, start_date_string = start_date_string, end_date_string = end_date_string)
            
        elif in_palmira:
            query = generic_sql.format(table_name = 'palmira', graph_id = graph_id, start_date_string = start_date_string, end_date_string = end_date_string)
            
        # Compute Weights
        nodes = utils.run_simple_query(self.client, query, allow_large_results = True)
        
        # Sets the value
        nodes.rename(columns = {'distance_to_infected':'value'}, inplace = True)
                
        # Adds the attribute name
        nodes['attribute_name'] = self.attribute_name
        
        # Returns the value
        return(nodes)
    
    
    
    

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
        
    
    
    
    
    
