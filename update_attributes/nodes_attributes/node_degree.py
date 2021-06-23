# Node Degree attribute


from node_attribute_generic import GenericNodeAttribute
import pandas as pd
import utils
import numpy as np
from datetime import timedelta

# Dictionary to include property values
property_values = {}

# Attribute name
property_values['attribute_name'] = 'node_degree'


class NodeDegree(GenericNodeAttribute):
    '''
    Script that computes the degree of the node
    '''

    def __init__(self):
        # Initilizes the super class
        GenericNodeAttribute.__init__(self, property_values)


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
        
        datset_id = self.df_locations.loc[location_id, 'dataset']

        # Transit dates
        start_transits_date_string = start_date_string
        end_transits_date_string = end_date_string

        # If static will collect all devices        
        if self.df_locations.loc[location_id, 'construction_type'] == utils.CT_STATIC:
            # Starts dates
            start_transits_date_string = pd.to_datetime(self.df_locations.loc[location_id, 'start_date'] ).strftime(utils.date_format)
            # End date. Substracts 1 day because end date in static scheme is not inlusive
            end_transits_date_string = (pd.to_datetime(self.df_locations.loc[location_id, 'end_date']) - timedelta(days = 1)).strftime(utils.date_format)

        
        query = f"""
                WITH contacts as (
                  SELECT id1,id2
                  FROM grafos-alcaldia-bogota.{datset_id}.{location_id}
                  WHERE date >= "{start_date_string}" AND date <= "{end_date_string}"
                  GROUP BY id1, id2),

                  nodes as
                  (
                    SELECT identifier
                    FROM grafos-alcaldia-bogota.transits.hourly_transits
                    WHERE location_id = "{location_id}"
                          AND date >= "{start_transits_date_string}" AND date <= "{end_transits_date_string}"
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
    
    
