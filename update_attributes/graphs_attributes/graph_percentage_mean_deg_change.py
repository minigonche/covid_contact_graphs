# Average distance to infected


from graph_attribute_generic_with_cases import GenericGraphAttributeWithCases
import pandas as pd
import numpy as np
import utils
import positive_db_functions as pos_fun



# This Class has multiple attributes names

# Dictionary to include property values
property_values = {}

# Attribute name
property_values['attribute_name'] = 'percetange_contact_change'


class GraphPercentageContactChange(GenericGraphAttributeWithCases):
    '''
    Script that computes the average distance to infected
    '''

    def __init__(self):
        # Initilizes the super class
        GenericGraphAttributeWithCases.__init__(self, property_values)
        

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
                - attribute_name (str): The attribute nam            
                - value (float): The value of the attribute
        '''
    
        raise ValueError('Should not enter here')
    
    
    def compute_attribute_for_interval(self, graph_id ):
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
                
        query = f"""
            SELECT AVG(attribute_value) as value
            FROM {utils.nodes_attribute_table}
            WHERE location_id = "{graph_id}" AND attribute_name = "distance_to_infected" AND date = "{end_date_string}"
        """
        
        df = utils.run_simple_query(self.client, query)
        
        # Extracts the value
        value = df['value'].values[0]
        
        if pd.isna(value):
            print('             ' + f'No distance to infected found for {graph_id} on {end_date_string}')
            return(pd.DataFrame({'value':None, 'attribute_name':[self.attribute_name] }))
        df_response = pd.DataFrame({'value':[value], 'attribute_name':[self.attribute_name] })
        
        return(df_response)


####################################################################################################################
df_graph_attributes = pd.read_csv('/Users/chaosdonkey06/Dropbox/covid_graphs/graph_attributes/graph_attributes.csv')

attributes_graph = ['number_of_cases_accumulated',
        'largest_eigenvalue_unweighted',
        'graph_size',
        'average_distance_to_infected',
        'graph_num_edges',
        'powerlaw_degree_ks_statistic',
        'powerlaw_degree_is_dist',
        'number_of_contacts',
        'largest_eigenvalue_weighted',
        'graph_transitivity',
        'pagerank_gini_index',
        'eigenvector_gini_index',
        'personalized_pagerank_gini_index',
        'powerlaw_degree_p_value',
        'powerlaw_degree_alpha']

df_city = df_graph_attributes.copy()
df_city = df_city.query("type=='city'")
df_city = df_city.set_index('location_id','attribute_value')
df = []
for location_id in set(df_city.index.values) :
    type_id = df_city.loc[location_id]['type'].unique()[0]
    df_location_id = df_city.loc[location_id].set_index('attribute_name')

    df_location_id         = df_location_id.copy().loc[['number_of_contacts', 'graph_num_edges', 'graph_size']] #, 'largest_eigenvalue_unweighted', 'largest_eigenvalue_weighted']]
    df_location_id['date'] = pd.to_datetime(df_location_id['date'])
    df_location_id         = df_location_id.sort_values(by='date')

    df_graph_attr_treatment = df_location_id.pivot_table(values='attribute_value', index='date', columns='attribute_name', aggfunc='first')
    num_contacts_baseline   = df_graph_attr_treatment['number_of_contacts'].iloc[:3].mean()
    graph_size_baseline     = df_graph_attr_treatment['graph_size'].iloc[:3].mean()
    df_graph_attr_treatment[ 'num_contacts_change_percentage' ]          = df_graph_attr_treatment.apply(lambda x: (x['number_of_contacts']-num_contacts_baseline)/num_contacts_baseline*100, axis=1)
    df_graph_attr_treatment[ 'num_contacts_change_percentage_weighted' ] = df_graph_attr_treatment.apply(lambda x: (x['number_of_contacts']-num_contacts_baseline)/num_contacts_baseline*x['graph_size']/graph_size_baseline*100, axis=1 ) 
    df_graph_attr_treatment = df_graph_attr_treatment.dropna()
    df_graph_attr_treatment['location_id'] = location_id
    df_graph_attr_treatment['type'] = type_id
    df.append(df_graph_attr_treatment)

df_response = pd.concat(df)

import matplotlib.pyplot as plt
fig, ax = plt.subplots(1,1, figsize=(15.5, 7))
ax.plot(df_graph_attr_treatment.index.values,
        df_graph_attr_treatment.num_contacts_change_percentage.values, 
        color='k',
        label='%')

ax.plot(df_graph_attr_treatment.index.values,
        df_graph_attr_treatment.num_contacts_change_percentage_weighted.values, 
        color='r',
        label=r'$\%$ graph_size weighted')

ax.set_xlabel('Date')
ax.set_ylabel('Cambio porcentual en los contactos [%]')
#ax.set_yscale('log')

ax.legend()
plt.show()