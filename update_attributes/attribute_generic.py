# Generic Attribute Class
# This class serves as the abstract class for computing attributes
import utils
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta


# Start date
# Can be edited by the subclass in case it is needed
# Must be sunday
starting_date = pd.to_datetime("2020-02-09") # Corresponds to the sixth week of the year


class GenericWeeklyAttribute():
    '''
    Generic Attribute class for weekly graphs.

    This class can be used for both nodes and graphs attributes
    '''


    # Initializer
    def __init__(self, attribute_name, starting_date = starting_date):
        '''
        Initializer of the class.

        The following parametrs must be provided:
            - attribute_name(str): The attribute Name
            - starting_date(pd.datetime)(optional): Can establish a starting date for the attribute

        '''

        # Starts the client
        self.client = bigquery.Client(location="US")

        # Saves the attribute_name
        self.attribute_name = attribute_name

        # Saves the starting date
        # Adjusts starting date so that is sunday (or else it will not be consistent with the database)
        self.starting_date = starting_date
        while self.starting_date.dayofweek != 6:
            self.starting_date = self.starting_date + timedelta(days = 1)            




    # --- Global Abstract Methods
    def compute_attribute(self, nodes, edges):
        '''
        # TODO
        Main Method to Implement
        
        This method must be implemented by the subclass. It receives compact nodes and edges and 
        must output the corresponding attribute. This method must return unique identifiers and it's good 
        practice to include all identifiers. In case its a graph attribute the identifier column is ignored

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
        raise NotImplementedError



    # -- Editable Methods (Probably)
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

        return(True)

        

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

        return(current_date >= self.starting_date)



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
        

        nodes = self.get_compact_nodes(graph_id, start_date_string, end_date_string)
        edges = self.get_compact_edgelist(graph_id, start_date_string, end_date_string)

        return(self.compute_attribute(nodes, edges))



    # --- Abstract Methods For the Attribute Type

    def save_attribute_for_week(self, graph_id, year, week):
        '''
        Method that computes the attribute for a given week and savess it in the database.
        All weeks are saved as the sunday and the go from monday to sunday.


        params
            - graph_id(str): The graph id
            - year (int): The year to compute the attribute, this shit could go until 2021
            - week (int): The week of the year to compute

        generates
            - Exception if the databaase already contains the attribute for the given week for the given graph id
            - Exception if the there is no support for the attribute type
        '''

        raise NotImplementedError


    # -- Other Methods

    def get_complete_edgelist(self, graph_id, start_date_string, end_date_string):
        '''
        Method that gets the edgelist of the location. Both dates are inclusive

        parameters
            graph_id(str): The graph id        
            start_date_string (str): Start date in %Y-%m-%d
            end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the ungrouped edglist. It has the same structure as the edglist table
        '''

        dataset_id = utils.get_dataset_of_location(self.client, graph_id)

        query = f"""

            SELECT *
            FROM grafos-alcaldia-bogota.{dataset_id}.{graph_id}
            WHERE date >= "{start_date_string}" AND date <= "{end_date_string}"

        """

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)


    def get_compact_edgelist(self, graph_id, start_date_string, end_date_string):
        '''
        Method that gets the compact edgelist of the location. Both dates are inclusive

        parameters
            graph_id(str): The graph id        
            start_date_string (str): Start date in %Y-%m-%d
            end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the grouped edglist.
                - id1 (str)
                - id2 (str)
                - weight (num) : sum of contacts
        '''
        dataset_id = utils.get_dataset_of_location(self.client, graph_id)

        query = f"""

            SELECT id1, id2, SUM(contacts) as weight
            FROM grafos-alcaldia-bogota.{dataset_id}.{graph_id}
            WHERE date >= "{start_date_string}" AND date <= "{end_date_string}"
            GROUP BY id1, id2

        """

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)




    def get_complete_nodes(self, graph_id, start_date_string, end_date_string):
        '''
        Method that extracts the ungrouped transits (nodes) given the location id

        parameters
            graph_id(str): The graph id        
            start_date_string (str): Start date in %Y-%m-%d
            end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the ungrouped nodes. It has the same structure as the transits table

        '''

        query = f"""

            SELECT *
            FROM grafos-alcaldia-bogota.transits.daily_transits
            WHERE location_id = "{graph_id}" 
                  AND date >= "{start_date_string}"
                  AND date <= "{end_date_string}"

        """

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)




    def get_compact_nodes(self, graph_id, start_date_string, end_date_string):
        '''
        Method that extracts the grouped transits (nodes) given the location id

        parameters
            - graph_id(str): The graph id
            - start_date_string (str): Start date in %Y-%m-%d
            - end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the ungrouped nodes.
                - identifier (str)
                - weight (int): the sum of the total_transits       
    
        '''

        query = f"""

            SELECT identifier, SUM(total_transits) as weight
            FROM grafos-alcaldia-bogota.transits.daily_transits
            WHERE location_id = "{graph_id}"
                  AND date >= "{start_date_string}" 
                  AND date <= "{end_date_string}"

            GROUP BY identifier

        """

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)











