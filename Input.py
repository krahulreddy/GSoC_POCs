import psycopg2
from psycopg2.extras import RealDictCursor
from Doc import Doc
import json


def connect_and_test_db():
    print("=================================================================")
    print("Trying to create a connection")
    try:
        connection = psycopg2.connect(
            user="nominatim",
            password="",              # Replace with your password while using
            host="127.0.0.1",
            port="5432",
            database="nominatim"
        )
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        print("Success")
    except:
        print("Failed")
        exit

    print("=================================================================")
    print("Trying to print DSN parameters: ")
    try:
        print(connection.get_dsn_parameters())
    except:
        print("Failed")
        exit

    print("=================================================================")
    print("Trying to fetch from placex table: ")

    try:
        sql = "SELECT name from placex \
where name->'name' like 'Monaco' limit 1;"
        cursor.execute(sql)
        record = cursor.fetchone()
        print(sql, "\n")
        print(json.dumps(record))

    except:
        print("Failed")
        exit

    print("=================================================================")
    print("Trying to create doc")

    sql = "SELECT place_id, osm_id, osm_type, name, address, \
country_code, housenumber, postcode from placex \
where name->'name' like 'Monaco' limit 1 "
    cursor.execute(sql)
    record = cursor.fetchone()
    print(sql, "\n")

    place_id, osm_id, osm_type, name, address, country_code, housenumber, \
        postcode = record.values()
    doc = Doc(place_id, osm_id, osm_type, name, address,
                  country_code, housenumber, postcode)

    print("osm_id: ", doc.osm_id)
    print("osm_type: ", doc.osm_type)

    print("=================================================================")
    print("Closing connection")
    cursor.close()


connect_and_test_db()

# test_doc()
