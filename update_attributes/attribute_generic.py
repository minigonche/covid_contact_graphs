# Generic Attribute Class
# This class serves as the abstract class for computing attributes
import utils
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


# Attribute Dictionary
# Declares all the necesarry attributes in a dictionary (Easier to include new ones)
# Set value to None, to force the subclass to implement it

# Attributes
# -----------------------------
default_property_values = {}

# Necessary
# ---------
# Attribute Name (must be implemented by subclass)
default_property_values['attribute_name'] = None


# Optional
# ---------
# Start Date of the attribute
default_property_values['starting_date'] = utils.global_min_attribute_date # GLobally assinged
# Max number of edges allow to proceed with calculation
default_property_values['max_num_nodes'] = np.inf
# Max number of nodes allow to proceed with calculation
default_property_values['max_num_edges'] = np.inf
# Priority of the attribute (lower -> first)
default_property_values['priority'] = 1


class GenericWeeklyAttribute():
    '''
    Generic Attribute class for weekly graphs.

    This class can be used for both nodes and graphs attributes
    '''
    # Initializer
    def __init__(self, edited_property_values):
        '''
        Initializer of the class.

        The following parameters must be provided:
            - edited_property_values : the property dictionary of the class. All None default values must be included

        '''
        
        # Starts the properties by the dictionary
        for k in default_property_values:
            
            # Sets default
            val = default_property_values[k]
            
            # Checks if given
            if k in edited_property_values:
                val = edited_property_values[k]
                
            if pd.isna(val):
                raise ValueError(f'The property {k} must be set by the subclass and was missing or was None, in edited_property_values parameter dictionary.')

            # Sets the attribute
            setattr(self, k, val)
                
                       
        # Edits the initial properties
                
        # Starts the client
        self.client = bigquery.Client(location="US")
                    
            
        # Extracts the locations
        self.df_locations = utils.get_current_locations(self.client)
        self.df_locations.index = self.df_locations.location_id
        
        # Extract locations min support date
        self.min_location_start_dates = utils.get_min_support_date_for_location_attributes(self.client)
        self.min_location_start_dates.index = self.min_location_start_dates.location_id
        
        
        # Extracts the sizes
        self.df_graph_sizes = utils.get_all_graph_sizes(self.client)
        self.df_graph_sizes.date = self.df_graph_sizes.date.apply(pd.to_datetime)
        self.df_graph_sizes.set_index(['location_id','date'], inplace = True)
        
        
        


    # --- Global Abstract Methods
    # -----------------------------------------------
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
    # -----------------------------------------------
    def location_id_supported(self, location_id):
        '''
        Method that determines if the attribute is supported for the location_id (graph).
        The default implementation is to return True, unless it has construction type: STATIC that checks if 
        the current date is after the end date of the delimiting dates.

        Overwrite this method in case the attribute is not on any date for a given location.
    
        NOTE: This method is called several times inside a loop. Make sure you don't acces any expensive resources in the implementation.
        
        params
            - location_id (str)

        returns
            Boolean
        '''

        # Checks if static and that all nodes are computed
        if self.df_locations.loc[location_id, 'construction_type'] == utils.CT_STATIC:
            if utils.get_today() < pd.to_datetime(self.df_locations.loc[location_id, 'end_date']):
                print(f"location: {location_id} is static. Will wait until {self.df_locations.loc[location_id, 'end_date']} to compute the attribute.")
                return(False)
                
        return(True)

        

    def location_id_supported_on_date(self, location_id, current_date):
        '''
        Method that determines if the attribute is supported for the location_id (graph) on a specific date
        The default implementation is to return True if the current date is equal or larger that the starting date of the attribute and of the location_id (if decalred in graph_attributes.min_support_dates).
        
        Also, num_edges and num_contacts must be bellow the defined treshod. 
    
        NOTE: This method is called several times inside a loop. Make sure you don't acces any expensive resources in the implementation.
        
        params
            - location_id (str)
            - current_date (pd.datetime): the current datetime

        returns
            Boolean
        '''
        # Uses date
        min_date = self.starting_date # Attribute date
        
        if location_id in self.min_location_start_dates.index:
            min_date = max(min_date, self.min_location_start_dates.loc[location_id, 'min_date'])
        
        support_date = current_date >= min_date
    
        if not support_date:
            print(f'               Min date is set to: {min_date.strftime("%y-%m-%d")}')
            return(False)


            
        # Uses the sizes
        num_edges = self.df_graph_sizes.loc[(location_id,current_date),'num_edges']
        num_nodes = self.df_graph_sizes.loc[(location_id,current_date),'num_nodes']
        
        support_size = num_edges <= self.max_num_edges and num_nodes <= self.max_num_nodes
        
        if not support_size:
            print(f'               Nodes: {num_nodes} and Edges: {num_edges} exceeds max: ({self.max_num_nodes},{self.max_num_edges})')
        
        return(support_size and support_date)



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
        

        nodes = self.get_compact_nodes(location_id, start_date_string, end_date_string)
        edges = self.get_compact_edgelist(location_id, start_date_string, end_date_string)

        return(self.compute_attribute(nodes, edges))

    # --- Abstract Methods For the Attribute Type
    # -----------------------------------------------
    
    # For next iteration
    def save_attribute_for_date(self, location_id, date_string):
        '''
        Method that computes the attribute for a given date and savess it in the database.
        For each date the previuos seven days are taken


        params
            - location_id(str): The graph id
            - date_string (str): Date in the format yyyy-mm-dd

        generates
            - Exception if the databaase already contains the attribute for the given date
            - Exception if the there is no support for the attribute type
        '''

        raise NotImplementedError
        
    def save_attribute_for_week(self, location_id, year, week):
        '''
        Method that computes the attribute for a given week and savess it in the database.
        All weeks are saved as the sunday and the go from monday to sunday.

        params
            - location_id(str): The graph id
            - date_string (str): Date in the format yyyy-mm-dd

        generates
            - Exception if the databaase already contains the attribute for the given date
            - Exception if the there is no support for the attribute type
        '''

        raise NotImplementedError        


    # -- Other Methods
    # -----------------------------------------------
    def get_complete_edgelist(self, location_id, start_date_string, end_date_string):
        '''
        Method that gets the edgelist of the location. Both dates are inclusive

        parameters
            location_id(str): The graph id        
            start_date_string (str): Start date in %Y-%m-%d
            end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the ungrouped edglist. It has the same structure as the edglist table
        '''

        dataset_id = utils.get_dataset_of_location(self.client, location_id)

        query = f"""

            SELECT *
            FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
            WHERE date >= "{start_date_string}" AND date <= "{end_date_string}"

        """

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)

    def get_compact_edgelist(self, location_id, start_date_string, end_date_string):
        '''
        Method that gets the compact edgelist of the location. Both dates are inclusive

        parameters
            location_id(str): The graph id        
            start_date_string (str): Start date in %Y-%m-%d
            end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the grouped edglist.
                - id1 (str)
                - id2 (str)
                - weight (num) : sum of contacts
        '''
        dataset_id = utils.get_dataset_of_location(self.client, location_id)

        query = f"""

            SELECT id1, id2, COUNT(*) as weight, SUM(contacts) as total_contacts
            FROM grafos-alcaldia-bogota.{dataset_id}.{location_id}
            WHERE date >= "{start_date_string}" AND date <= "{end_date_string}"
            GROUP BY id1, id2

        """
        
        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)




    def get_complete_nodes(self, location_id, start_date_string, end_date_string):
        '''
        Method that extracts the ungrouped transits (nodes) given the location id. If the
        construction type is static, will invoke the entire duration.

        parameters
            location_id(str): The graph id        
            start_date_string (str): Start date in %Y-%m-%d
            end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the ungrouped nodes. It has the same structure as the transits table

        '''

        # If static will collect all devices        
        if self.df_locations.loc[location_id, 'construction_type'] == utils.CT_STATIC:
            # Starts dates
            start_date_string = pd.to_datetime(self.df_locations.loc[location_id, 'start_date'] ).strftime(utils.date_format)
            # End date. Substracts 1 day because end date in static scheme is not inlusive
            end_date_string = (pd.to_datetime(self.df_locations.loc[location_id, 'end_date']) - timedelta(days = 1)).strftime(utils.date_format)

        query = f"""

                SELECT *
                FROM grafos-alcaldia-bogota.transits.hourly_transits
                WHERE location_id = "{location_id}" 
                    AND date >= "{start_date_string}"
                    AND date <= "{end_date_string}"

            """

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)




    def get_compact_nodes(self, location_id, start_date_string, end_date_string):
        '''
        Method that extracts the grouped transits (nodes) given the location id. If the
        construction type is static, will invoke the entire duration.

        parameters
            - location_id(str): The graph id
            - start_date_string (str): Start date in %Y-%m-%d
            - end_date_string (str): End date in %Y-%m-%d

        returns
            pd.DataFrame with the ungrouped nodes.
                - identifier (str)
                - weight (int): the sum of the total_transits       
    
        '''
        # If static will collect all devices        
        if self.df_locations.loc[location_id, 'construction_type'] == utils.CT_STATIC:
            # Starts dates
            start_date_string = pd.to_datetime(self.df_locations.loc[location_id, 'start_date'] ).strftime(utils.date_format)
            # End date. Substracts 1 day because end date in static scheme is not inlusive
            end_date_string = (pd.to_datetime(self.df_locations.loc[location_id, 'end_date']) - timedelta(days = 1)).strftime(utils.date_format)


        query = f"""

            SELECT identifier, SUM(total_transits) as weight
            FROM grafos-alcaldia-bogota.transits.hourly_transits
            WHERE location_id = "{location_id}"
                  AND date >= "{start_date_string}" 
                  AND date <= "{end_date_string}"

            GROUP BY identifier

        """

        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        query_job = self.client.query(query, job_config=job_config) 

        # Return the results as a pandas DataFrame
        df = query_job.to_dataframe()

        return(df)











