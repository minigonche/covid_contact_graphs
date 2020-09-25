# Node Betweenness


from node_attribute_generic import GenericNodeAttribute
import pandas as pd
import igraph as ig
import utils
import numpy as np

attribute_name = 'betweenness_centrality'

# Max Support
max_num_nodes = np.inf
max_num_edges = 50000000 # 50 Millions


class NodeBetweenness(GenericNodeAttribute):
    '''
    Script that computes the betweenness of the nodes
    '''

    def __init__(self):
        # Initilizes the super class
        GenericNodeAttribute.__init__(self, attribute_name = attribute_name, max_num_nodes = max_num_nodes, max_num_edges = max_num_edges)
                

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

        # Create the graph
        G = ig.Graph()
        
        # Adds the values
        G.add_vertices(nodes.identifier.values)
        
        if edges.shape[0] > 0:
            G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
        
        # Exctracs the betweenness
        betweenness = G.betweenness(weights =  edges.weight.values, directed = False)
        
        # Adds it to the nodes
        nodes['value'] = betweenness
        
        # Adds the attribute name
        nodes['attribute_name'] = self.attribute_name
        
        # Constructs the dataframe
        return(nodes)
    

        
    
    