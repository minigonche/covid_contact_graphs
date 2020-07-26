# Node pagerank


from node_attribute_generic import GenericNodeAttribute
import pandas as pd
import igraph as ig
import utils

attribute_name = 'pagerank_centrality'

class NodePageRank(GenericNodeAttribute):
    '''
    Script that computes the pagerank of the nodes
    '''

    def __init__(self):
        # Initilizes the super class
        GenericNodeAttribute.__init__(self, attribute_name)
                

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

        # Adds weights
        G.es['weight'] = edges.weight.values
        
        if edges.shape[0] > 0:
            G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))
        
        # Exctracs the pagerank
        page_rank = G.pagerank(weights = 'weight',  directed = False)
        
        # Adds it to the nodes
        nodes['value'] = page_rank
        
        # Adds the attribute name
        nodes['attribute_name'] = self.attribute_name
        
        # Constructs the dataframe
        return(nodes)
    
    
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
        
        # For medellin also include week 2
        if location_id == 'colombia_medellin' and current_date >= utils.hell_week_2[0] and current_date <= utils.hell_week_2[1]:
            return(False)
        
        # For valle_del_cauca also include week 2
        if location_id == 'colombia_valle_del_cauca' and current_date >= utils.hell_week_2[0] and current_date <= utils.hell_week_2[1]:
            return(False) 
        
        
        
        return(True)
        
    
    
    
    
    
    
