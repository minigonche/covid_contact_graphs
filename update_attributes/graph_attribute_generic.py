# Generic Graph Attribute

from attribute_generic import GenericWeeklyAttribute
import attribute_generic
import utils
from datetime import datetime, timedelta
import pandas as pd

class GenericGraphAttribute(GenericWeeklyAttribute):
    '''
    Class for the Generic Graph Attribute
    '''

    def __init__(self, attribute_name, starting_date = attribute_generic.starting_date):
        # Initilizes the super class
        GenericWeeklyAttribute.__init__(self, attribute_name,  starting_date)



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
            - Exception if the the result does not contain the defined  strucure. See GenericWeeklyAttribute.compute_attribute
        '''

        date_time = utils.get_date_of_week(year, week)
        date_string = date_time.strftime( utils.date_format)

        if utils.graph_attribute_exists(self.client, graph_id, self.attribute_name, date_string):
            raise ValueError(f'Attribute: {self.attribute_name} already exists for {date_string}')


        # Goes back 6 days (to monday)
        start_date_string = (date_time - timedelta(days = 6)).strftime( utils.date_format)
        end_date_string = date_string

        # Computes
        df_result = self.compute_attribute_for_interval(graph_id, start_date_string, end_date_string)

        if 'value' not in df_result.columns:
            raise ValueError(f'The column "value" was not found in the columns {df_result.columns}')
        
        if 'attribute_name' not in df_result.columns:
            raise ValueError(f'The column "attribute_name" was not found in the columns {df_result.columns}')
            
            
        df_result.rename(columns = {'value':'attribute_value'}, inplace = True)
        # Adds the columns
        df_result['location_id'] = graph_id
        df_result['date'] = date_string
        df_result['type'] = self.df_locations.loc[graph_id, 'type']
    
        df_result = df_result[['location_id','date','attribute_name','attribute_value','type']]
        
        # Sets the types
        df_result.location_id = df_result.location_id.astype(str)
        df_result['type'] = df_result['type'].astype(str)
        df_result.date = df_result.date.apply(lambda d: pd.to_datetime(d))
        df_result.attribute_name = df_result.attribute_name.astype(str)
        df_result.attribute_value = df_result.attribute_value.astype(float)
        
        utils.insert_graphs_attributes(self.client, df_result)
