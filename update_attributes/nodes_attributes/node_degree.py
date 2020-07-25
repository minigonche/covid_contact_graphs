# Node Degree attribute


from node_attribute_generic import GenericNodeAttribute
import pandas as pd
import utils

attribute_name = 'node_degree'

class NodeDegree(GenericNodeAttribute):
    '''
    Script that computes the degree of the node
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
                WITH contacts as (
                  SELECT id1,id2
                  FROM grafos-alcaldia-bogota.{datset_id}.{graph_id}
                  WHERE date >= "{start_date_string}" AND date <= "{end_date_string}"
                  GROUP BY id1, id2),

                  nodes as
                  (
                    SELECT identifier
                    FROM grafos-alcaldia-bogota.transits.hourly_transits
                    WHERE location_id = "{graph_id}"
                          AND date >= "{start_date_string}" AND date <= "{end_date_string}"
                   GROUP BY identifier
                  )


                SELECT identifier, (COUNT(*) - 2) as degree
                FROM 
                (SELECT identifier FROM nodes LEFT JOIN contacts ON nodes.identifier = contacts.id2
                UNION ALL
                SELECT identifier FROM nodes LEFT JOIN contacts ON nodes.identifier = contacts.id2)
                GROUP BY identifier
                ORDER BY  degree, identifier
        """
        
        df = utils.run_simple_query(self.client, query, allow_large_results = True)
        df.rename(columns = {'degree':'value'}, inplace = True)
        df['attribute_name'] = self.attribute_name

        return(df)
    
    
