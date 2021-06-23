# Checks the integrity if the pipeline before starting
from functions.utils import CONSTRUCTION_TYPES, CT_STATIC
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import pandas as pd


# Custom Scripts
import utils
        

new_line = "\n"
        
def main():
    
    print("Checks Integrity.")
    # Gets the coverage
    client = bigquery.Client(location="US")

    # Duplicate records in location_geometries
    # ----------------------------------------
    print("   Checking for Duplicated Records")
    df_duplicates = utils.get_duplicate_locations(client)

    if df_duplicates.shape[0] > 0:
        print('FOUND DUPLICATE LOCATIONS')
        for _, row in df_duplicates.iterrows():
            print(f"   {row.location_id}, Num. Records: {row.tot}")
        
        raise ValueError("Please correct these locations and try again")
    
    print("      OK: No duplicate records found")


    print("   Checking for Construction Types")
    # Checks that all locations have the construction type
    # ---------------------------
    df_locations = utils.get_current_locations_complete(client)
    problems = []
    for _, row in df_locations.iterrows():
        if row.construction_type not in CONSTRUCTION_TYPES:
            problems.append(f"{row.location_id}: {row.construction_type}")

    if len(problems) > 0:
        raise ValueError(f"The following locations have unsupported construction_type: {new_line.join(problems)}")

    print("      OK: Construction types are OK")



    print("   Checking for STATIC locations")
    # Checks that static locations have start and end date
    # --------------------------------
    problems = []
    for _, row in df_locations.iterrows():
        if row.construction_type == CT_STATIC:
            if pd.isna(row['start_date']):
                problems.append(f"{row.location_id}: start date is null")
            
            elif pd.isna(row['end_date']):
                problems.append(f"{row.location_id}: end date is null")
            
            elif row['end_date'] <= row['start_date']:
                problems.append(f"{row.location_id}: end date has to be after start date")
                    
    if len(problems) > 0:
        raise ValueError(f"The following static locations have problems: {new_line.join(problems)}")

    print("      OK: STATIC locations are consistent")



    
if __name__ == "__main__":
    
    # Exceute Main
    main()