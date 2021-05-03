# Node personalized pagerank


from node_attribute_generic_with_cases import GenericNodeAttributeWithCases
import pandas as pd
import numpy as np
import igraph as ig
import utils
import positive_db_functions as pos_fun


# Dictionary to include property values
property_values = {}

# Attribute name
property_values['attribute_name'] = 'personalized_pagerank_centrality'

# Priority
property_values['priority'] = 2

# Max Support
property_values['max_num_nodes'] = np.inf
property_values['max_num_edges'] = 25000000 # 25 Millions


# Other properties

# Epsilon
eps = 1e-16
# Constant for division (For weight depending on distance)
div = 1200 # in meters

class NodePersonalizedPageRank(GenericNodeAttributeWithCases):
    '''
    Script that computes the pagerank of the nodes
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
                        
        query = f"""
             SELECT identifier, attribute_value as distance_to_infected
                FROM {utils.nodes_attribute_table}
                WHERE location_id = '{location_id}'
                    AND date = '{end_date_string}'
        """
        
        # Compute Weights
        df_distances = utils.run_simple_query(self.client, query, allow_large_results = True)
        
        if df_distances.shape[0] == 0:
            raise ValueError(f'No distance to infected found for {location_id} on {end_date_string}. Please compute it!')
        
        # Sets Nones
        df_distances.fillna(np.inf, inplace = True)
        
        # Apply inverted soft_plus
        df_distances['dist_weight'] = np.log(1 + np.exp(-1*df_distances.distance_to_infected/div))/np.log(2)
         
        
        # Nodes
        nodes = self.get_compact_nodes(location_id, start_date_string, end_date_string)
        
        # Merges with weights        
        nodes = nodes.merge(df_distances, on = 'identifier', how = 'left')
        #print(nodes.dist_weight.isna().sum())
        nodes.fillna(0, inplace = True)
        
        # Edges               
        edges = self.get_compact_edgelist(location_id, start_date_string, end_date_string)    
        
        # Create the graph
        G = ig.Graph()
        
        # Adds the values
        G.add_vertices(nodes.identifier.values)        
                    
        if edges.shape[0] > 0:
            G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
        
        # Adds weights to edges
        G.es['weight'] = edges.weight.values
        
        # Exctracs the personalized pagerank
        personalized_page_rank = G.personalized_pagerank(weights = 'weight', directed = False, reset = nodes['dist_weight'].values)
        
        # Adds it to the nodes
        nodes['value'] = personalized_page_rank
        
        # Adds the attribute name
        nodes['attribute_name'] = self.attribute_name
        
        # Returns the value
        return(nodes)
    
    