# Generic Node Attribute

from attribute_generic import GenericWeeklyAttribute
import utils
from datetime import datetime, timedelta

class GenericNodeAttribute(GenericWeeklyAttribute):
    '''
    Class for the Generic Graph Attribute
    '''

    def __init__(self, attribute_name):
        # Initilizes the super class
        GenericWeeklyAttribute.__init__(self, attribute_name):



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

        date_time = util.get_date_of_week(year, week)
        date_string = date_time.strftime( utils.date_format)

        if util.node_attribute_exists(self.client, graph_id, self.attribute_name, date_string):

            raise ValueError(f'Attribute: {self.attribute_name} already exists for {date_string}')

        # Goes back 7 days
        start_date_string = (date_time - timedelta(days = 6)).strftime( utils.date_format)
        end_date_string = date_string

        # Computes
        df_result = self.compute_attribute_for_interval(graph_id, start_date_string, end_date_string)

        if 'value' not in df_result.columns:
            raise ValueError(f'The column "value" was not found in the columns {df_result.columns}')

        if 'identifier' not in df_result.columns:
            raise ValueError(f'The column "identifier" was not found in the columns {df_result.columns}')

        df_results.rename(columns {'value':'attribute_value'}, inplace = True)

        # Adds the columns
        df_result['location_id'] = graph_id
        df_result['date'] = date_string
        df_result['attribute_name'] = self.attribute_name

        df_result = df_result[['identifier','location_id','date','attribute_name','attribute_value']]

        util.insert_node_attributes(seld.client, df_result)
