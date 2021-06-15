# Script that deletes a given polygon from the pipeline

import sys
from google.cloud import bigquery
client = bigquery.Client(location="US")

# Customimports
import functions.utils as utils


from datetime import datetime, timedelta


# Reads the location_id
project_id = "grafos-alcaldia-bogota"



def main(location_id, remove_from_geo = False):

    print(f"Gets {location_id} info")
    sql = f"""

    SELECT *
    FROM grafos-alcaldia-bogota.geo.locations_geometries
    WHERE location_id = "{location_id}"
    """

    df = utils.run_simple_query(client, sql)

    if df.shape[0] == 0:
        raise ValueError(f"No information found for polygon: {location_id}")

    if df.shape[0] > 1:
        raise ValueError(f"Multiple information found for polygon: {location_id}. Inconsistent table, please check.")


    edge_list_dataset = df.loc[0,'dataset']

    print("Delete Transits")
    # Deletes transits
    sql = f"""

        DELETE FROM grafos-alcaldia-bogota.transits.hourly_transits
        WHERE location_id = "{location_id}"
    """

    utils.run_simple_query_no_result(client, sql)


    print("Delete Edges")

    # Deletes
    client.delete_table(f"{project_id}.{edge_list_dataset}.{location_id}", not_found_ok=True)  # Make an API request.


    print("Deletes Graph Sizes")
    # Deletes graph sizes
    sql = f"""

        DELETE FROM grafos-alcaldia-bogota.graph_attributes.graph_sizes
        WHERE location_id = "{location_id}"
    """

    utils.run_simple_query_no_result(client,sql)


    print("Deletes Graph Movement")
    # Deletes graph movement
    sql = f"""

        DELETE FROM grafos-alcaldia-bogota.graph_attributes.graph_movement
        WHERE location_id = "{location_id}"
    """

    utils.run_simple_query_no_result(client,sql)


    print("Deletes graph attribute")

    print("   Nodes")
    sql = f"""

        DELETE FROM grafos-alcaldia-bogota.graph_attributes.node_attributes
        WHERE location_id = "{location_id}"
    """

    utils.run_simple_query_no_result(client,sql)

    print("   Graphs")
    sql = f"""

        DELETE FROM grafos-alcaldia-bogota.graph_attributes.graph_attributes
        WHERE location_id = "{location_id}"
    """

    utils.run_simple_query_no_result(client,sql)


    if remove_from_geo:

        print("Deletes Geometry")
        stop()
        # Deletes graph movement
        sql = f"""

            DELETE FROM grafos-alcaldia-bogota.geo.locations_geometries
            WHERE location_id = "{location_id}"
        """

        utils.run_simple_query_no_result(client,sql)

    print('Done')
    print("----------------------")



if __name__ == "__main__":
    
    #location_id = sys.argv[1]

    remove_from_geo = False
    if len(sys.argv) >= 3:
        remove_from_geo = sys.argv[2].upper() == "TRUE"

    arr = ['colombia_bogota_poligono_salud_2',
            'colombia_bogota_poligono_salud_1',
            'colombia_bogota_poligono_salud_3',
            'colombia_bogota_poligono_salud_4',
            'colombia_bogota_poligono_salud_bosa',
            'colombia_bogota_study_9',
            'colombia_bogota_super_spreading_6',
            'colombia_bogota_study_10',
            'colombia_bogota_study_8',
            'colombia_bogota_study_7',
            'colombia_bogota_study_6',
            'colombia_bogota_super_spreading_1',
            'colombia_bogota_super_spreading_8',
            'colombia_bogota_super_spreading_4',
            'colombia_bogota_super_spreading_2']

    for location_id in arr:
        print(location_id)
        main(location_id, remove_from_geo = False)
        print()