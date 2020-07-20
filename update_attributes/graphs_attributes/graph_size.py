# Graph Size attribute


from graph_attribute_generic import GenericGraphAttribute
import pandas as pd

attribute_name = 'graph_size'

starting_date = pd.to_datetime("2020-06-28")


class GraphSize(GenericGraphAttribute):
    '''
    Script that computes the size of the graph
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, attribute_name, starting_date)


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
    
    
    def location_id_supported(self, location_id, current_date):
        '''
        Method that determines if the attribute is supported for the location_id (graph)
        The default implementation is to return True if the current date is equal or larger that the starting_date.
        Overwrite this method in case the attribute is not supported for a certain location_id (or several) at a particular date
    
        NOTE: This method is called several times inside a loop. Make sure you don't acces any expensive resources in the implementation.
        
        params
            - location_id (str)
            - current_date (pd.datetime): the current datetime

        returns
            Boolean
        '''
        
        if location_id != 'colombia_boyaca':
            return(False)
        
        return(current_date >= self.starting_date)