# Largest Eigen value of teh graph


from graph_attribute_generic import GenericGraphAttribute
import pandas as pd
import numpy as np
import igraph as ig
import utils

attribute_name = 'largest_eigenvalue_unweighted'

# Max Support
max_num_nodes = np.inf
max_num_edges = 50000000 # 50 Millions

class GraphEigenValueUnweighted(GenericGraphAttribute):
    '''
    Script that computes the dominant eigen value
    
    This script uses the results from the pagerank node attribute. If nothing is found will return None and writes a warning
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, attribute_name = attribute_name, max_num_nodes = max_num_nodes, max_num_edges = max_num_edges)
                        

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
                - weight (num) Weight of the edge (see get_compact_edgelist)      

        
        returns
            pd.DataFrame with the following structure
                - attribute_name (str): The attribute name           
                - identifier (str): Identifier of the node or graph
                - value (float): The value of the attribute
        '''

        # Create the graph
        G = ig.Graph()
        
        # Adds the values
        G.add_vertices(nodes.identifier.values)
                   
        if edges.shape[0] > 0:
            G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
        
        # Exctracs the eigen value
        _, eigen_value = G.evcent(directed = False, return_eigenvalue = True)
        
        # Creates the response
        df_response = pd.DataFrame({'value':[eigen_value], 'attribute_name':[self.attribute_name] })
        
        return(df_response)
    
    

    