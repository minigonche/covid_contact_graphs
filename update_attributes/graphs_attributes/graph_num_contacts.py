# Graph Size attribute


from graph_attribute_generic import GenericGraphAttribute
import pandas as pd
import utils

attribute_name = 'number_of_contacts'

class GraphNumberOfContacts(GenericGraphAttribute):
    '''
    Script that computes the number of unique contacts (by hour) for the total week
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttribute.__init__(self, attribute_name)
        
        # Extracts the locations
        self.df_locations = utils.get_current_locations(self.client)
        self.df_locations.index = self.df_locations.location_id        
                

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
                
        datset_id = self.df_locations.loc[graph_id, 'dataset']
        
        query = f"""
            SELECT COUNT(*) as num_contacts
            FROM grafos-alcaldia-bogota.{datset_id}.{graph_id}
            WHERE date >= "{start_date_string}" AND date <= "{end_date_string}"
        """
        
        df = utils.run_simple_query(self.client, query)
        df.rename(columns = {'num_contacts':'value'}, inplace = True)
        df['attribute_name'] = self.attribute_name

        return(df)
    