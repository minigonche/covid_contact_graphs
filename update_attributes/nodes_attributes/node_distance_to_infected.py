# Node distance to infected


from node_attribute_generic_with_cases import GenericNodeAttributeWithCases
import pandas as pd
import numpy as np
import igraph as ig
import utils
import positive_db_functions as pos_fun

#NOTA
# Los intervalos de dias a incluir según fecha de inicio sintomas, fueron ajustados después de
# una conversación con Mauricio

# Dictionary to include property values
property_values = {}

# Attribute name
property_values['attribute_name'] = 'distance_to_infected'
property_values['priority'] = 2

# Queries
# ---------
# Generic Query
generic_sql = """

        with 
        -- Graph Ids
        graph_ids as (
             SELECT identifier
             FROM grafos-alcaldia-bogota.transits.hourly_transits
             WHERE location_id = "{location_id}"
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
                     WHERE location_id = "{location_id}"
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



class NodeDistanceToInfected(GenericNodeAttributeWithCases):
    '''
    Script that computes the distance to infected
    '''

    def __init__(self):
        # Initilizes the super class
        GenericNodeAttributeWithCases.__init__(self, property_values)        
    

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
            query = bogota_sql.format(location_id = location_id, start_date_string = start_date_string, end_date_string = end_date_string)
            
        else:
            query = generic_sql.format(table_name = city, location_id = location_id, start_date_string = start_date_string, end_date_string = end_date_string)
            
        # Compute Weights
        nodes = utils.run_simple_query(self.client, query, allow_large_results = True)
        
        # Sets the value
        nodes.rename(columns = {'distance_to_infected':'value'}, inplace = True)
                
        # Adds the attribute name
        nodes['attribute_name'] = self.attribute_name
        
        # Returns the value
        return(nodes)
    
    
    
    
    
    
    
