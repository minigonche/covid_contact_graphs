# Node personalized pagerank


from node_attribute_generic import GenericNodeAttribute
import pandas as pd
import numpy as np
import igraph as ig
import utils

attribute_name = 'personalized_pagerank_centrality'

# Epsilon
eps = 1e-16
# Constant for division (For weight depending on distance)
div = 1200 # in meters

class NodePersonalizedPageRank(GenericNodeAttribute):
    '''
    Script that computes the pagerank of the nodes
    '''

    def __init__(self):
        # Initilizes the super class
        GenericNodeAttribute.__init__(self, attribute_name)
            
        self.df_codes =  utils.get_geo_codes(self.client, location_id = None)
        self.df_codes.index = self.df_codes.location_id
    

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
                
            
            
        query = f"""
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
                     AND loc.week_date <= "{end_date_string}"  -- Casas del ultimo mes
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
                  where DATE(fec_con_cast) >= DATE_SUB(DATE("{end_date_string}"), INTERVAL 30 DAY) ) as infectados -- Tabla con los infectados (del ultimo mes)
                 ) as d -- Matriz de distancia entre las casas y los infectados
                 GROUP BY identifier -- Agrupa por el identificador para encontrar la minima distancia
                 )


                 -- Final Query
                 SELECT graph_ids.identifier as identifier, distances.distance_to_infected as distance_to_infected
                 FROM graph_ids
                 LEFT JOIN distances
                 ON graph_ids.identifier = distances.identifier
        """
        
        # Compute Weights
        df_distances = utils.run_simple_query(self.client, query, allow_large_results = True)
        
        # Sets Nones
        df_distances.fillna(np.inf, inplace = True)
        
        # Apply inverted soft_plus
        df_distances['dist_weight'] = np.log(1 + np.exp(-1*df_distances.distance_to_infected/div))/np.log(2)
         
        
        # Nodes
        nodes = self.get_compact_nodes(graph_id, start_date_string, end_date_string)
        
        # Merges with weights        
        nodes = nodes.merge(df_distances, on = 'identifier', how = 'left')
        #print(nodes.dist_weight.isna().sum())
        nodes.fillna(0, inplace = True)
        
        # Edges               
        edges = self.get_compact_edgelist(graph_id, start_date_string, end_date_string)    
        
        # Create the graph
        G = ig.Graph()
        
        # Adds the values
        G.add_vertices(nodes.identifier.values)        
            
        # Adds weights to edges
        G.es['weight'] = edges.weight.values
        
        if edges.shape[0] > 0:
            G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
        
        # Exctracs the personalized pagerank
        personalized_page_rank = G.personalized_pagerank(weights = 'weight', directed = False, reset = nodes['dist_weight'].values)
        
        # Adds it to the nodes
        nodes['value'] = personalized_page_rank
        
        # Adds the attribute name
        nodes['attribute_name'] = self.attribute_name
        
        # Returns the value
        return(nodes)
    
    
    
    

    def location_id_supported(self, location_id):
        '''
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
        
        in_bogota = self.df_codes.loc[[location_id]].code_depto.apply(lambda c: c not in utils.bogota_codes).sum() == 0
        
        return(in_bogota)
    
    
    
    
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
                
        return(self.location_id_supported(location_id))
        
    
    
    
    
    
    
