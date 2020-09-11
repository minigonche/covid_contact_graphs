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
                    WHERE fechainici <> ''
                """


# Max Symptoms
generic_sql_max_date = """
                        SELECT MAX(DATE( date_start_symtoms)) as max_date
                        FROM  positives.{table_name}
                """



def get_positive_max_dates(client):
    '''
    Method that gets the positives max dates
    '''
    
    max_dates = {}
    max_dates['bogota'] = pd.to_datetime(utils.run_simple_query(client, bogota_sql_max_date).max_date.iloc[0])
    max_dates['palmira'] = pd.to_datetime(utils.run_simple_query(client, generic_sql_max_date.format(table_name = 'palmira')).max_date.iloc[0])
    
    return(max_dates)


def has_positives_database(client, location_id, df_codes = None):
    '''
    Method that checks if the location_id has positive seamples database
    '''
        
    in_bogota = utils.is_in_bogota(client, location_id, df_codes = df_codes)
    in_palmira = utils.is_in_palmira(client, location_id, df_codes = df_codes)
        
    return(in_bogota or in_palmira)


def positives_up_to_date(client, location_id, date, df_codes = None, max_dates = None):
    '''
    Checks that the positives in the database are up to date to excecute some procedure
    '''
    
    if max_dates is None:
        max_dates = get_positive_max_dates(client)
    
    in_bogota = utils.is_in_bogota(client, location_id, df_codes = df_codes)
    in_palmira =  utils.is_in_palmira(client, location_id, df_codes = df_codes)
        
    if in_bogota:
        return( (max_dates['bogota'] - pd.to_datetime(date)).days >= days_delay)
        
    if in_palmira:
        return( (max_dates['palmira'] - pd.to_datetime(date)).days >= days_delay)
        
    return(False)