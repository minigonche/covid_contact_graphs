# Graph Size attribute


from graph_attribute_generic import GenericGraphAttribute


parameter_name = 'graph_size'


class GraphSize(GenericGraphAttribute):
    '''
    Script that computes the size of the graph
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, attribute_name)


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
                - value (float): The value of the attribute
        '''
        

        df = pd.DataFrame({'value':[nodes.shape[0]]})

        return(df)