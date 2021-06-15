# Checks the integrity if the pipeline before starting
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest



# Custom Scripts
import utils
        
        
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


    
if __name__ == "__main__":
    
    # Exceute Main
    main()