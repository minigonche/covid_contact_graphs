# Positives Database Support
# Functions for support of positives database 

import utils
import numpy as np
import pandas as pd



# Delays in days from the symptom start date
days_delay = 3


# Max Symptoms
bogota_sql_max_date = """
                    SELECT MAX(DATE(TIMESTAMP(fechainici))) as max_date
                    FROM `servinf-unacast-prod.AlcaldiaBogota.positivos_agg_fecha`
                    WHERE CHAR_LENGTH(fechainici) = 10
                """


# Max Symptoms
generic_sql_max_date = """
                        SELECT MAX(DATE( date_start_symtoms)) as max_date
                        FROM  positives.{table_name}
                """

# Min Symptoms
bogota_sql_min_date = """
                    SELECT MIN(DATE(TIMESTAMP(fechainici))) as min_date
                    FROM `servinf-unacast-prod.AlcaldiaBogota.positivos_agg_fecha`
                    WHERE CHAR_LENGTH(fechainici) = 10
                """

# Min Symptoms
generic_sql_min_date = """
                        SELECT MIN(DATE( date_start_symtoms)) as min_date
                        FROM  positives.{table_name}
                """


## Cities with cases support
cities_with_positives = ['bogota','palmira','cucuta']

def get_positive_max_dates(client):
    '''
    Method that gets the positives max dates
    '''
    
    max_dates = {}
    max_dates['bogota'] = pd.to_datetime(utils.run_simple_query(client, bogota_sql_max_date).max_date.iloc[0])
    
    for city in cities_with_positives:
        if city != 'bogota':            
            max_dates[city] = pd.to_datetime(utils.run_simple_query(client, generic_sql_max_date.format(table_name = city)).max_date.iloc[0])

    
    return(max_dates)



def get_positive_min_dates(client):
    '''
    Method that gets the positives min dates
    '''
    
    min_dates = {}
    min_dates['bogota'] = pd.to_datetime(utils.run_simple_query(client, bogota_sql_min_date).min_date.iloc[0])
    
    for city in cities_with_positives:
        if city != 'bogota':            
            min_dates[city] = pd.to_datetime(utils.run_simple_query(client, generic_sql_min_date.format(table_name = city)).min_date.iloc[0])
            
    return(min_dates)


def has_positives_database(client, location_id, df_codes = None):
    '''
    Method that checks if the location_id has positive seamples database
    '''
        
    city = utils.get_city(client, location_id, df_codes = df_codes)
            
    return(city in cities_with_positives)


def positives_inside_date(client, location_id, date, df_codes = None, max_dates = None, min_dates = None):
    '''
    Checks that the positives in the database are up to date to excecute some procedure. Assumes the location has posiyive support
    '''
    
    if max_dates is None:
        max_dates = get_positive_max_dates(client)
        
    if min_dates is None:
        min_dates = get_positive_min_dates(client)        
        
    city = utils.get_city(client, location_id, df_codes = df_codes)
    
    before_max = (max_dates[city] - pd.to_datetime(date)).days >= days_delay
    after_min = (pd.to_datetime(date) - min_dates[city]).days >= days_delay
    
    return(before_max and after_min)


#####
# ------------
# --- Attribute with positives generic methods
# They are ment to be invoked by attributes that deal woth positive cases

def location_id_supported(client, location_id, df_codes):
    '''
    OVERWRITTEN
    # ---------------

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

    return( has_positives_database(client, location_id, df_codes))



def location_id_supported_on_date(client, location_id, current_date, df_codes, max_dates, min_dates):
    '''
    OVERWRITTEN
    # --------------

    Method that determines if the attribute is supported for the location_id (graph) on a specific date
    The default implementation is to return True if the current date is equal or larger that the starting_date and is not inside hell week
    Overwrite this method in case the attribute is not supported for a certain location_id (or several) at a particular date

    NOTE: This method is called several times inside a loop. Make sure you don't acces any expensive resources in the implementation.

    params
        - location_id (str)
        - current_date (pd.datetime): the current datetime

    returns
        Boolean
    '''

    up_to_date = positives_inside_date(client, location_id, current_date, df_codes, max_dates, min_dates)

    return(up_to_date) 