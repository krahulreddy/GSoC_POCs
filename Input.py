import psycopg2
from psycopg2.extras import RealDictCursor, DictCursor, register_hstore
from Doc import Doc
import json


def connect_and_test_db():
    connection = connect_to_db()
    get_dsn_parameters(connection)
    fetch_test(connection)
    fetch_and_create_doc(connection)
    return connection



def connect_to_db():
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
        register_hstore(connection, globally=True, unicode=True)
        print("Success")
        return connection
    except:
        print("Failed")
        exit

def get_dsn_parameters(connection):
    print("=================================================================")
    print("Trying to print DSN parameters: ")
    try:
        print(connection.get_dsn_parameters())
    except:
        print("Failed")
        exit

def fetch_test(connection):
    print("=================================================================")
    print("Trying to fetch from placex table: ")

    try:
        sql = "SELECT name from placex \
where name->'name' like 'Monaco' limit 1;"
        cursor = connection.cursor(cursor_factory=DictCursor)
        cursor.execute(sql)
        record = cursor.fetchone()
        print(sql, "\n")
        print(json.dumps(record))
        cursor.close()

    except:
        print("Failed")
        exit

def fetch_and_create_doc(connection, name="Monaco"):
    print("=================================================================")
    print("Trying to fetch row and create doc")

    sql = "SELECT place_id, osm_id, osm_type, name, address, \
country_code, housenumber, postcode from placex \
where name->'name' like '" + name + "' limit 1 "
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(sql)
    record = cursor.fetchone()
    print(sql, "\n")

    # place_id, osm_id, osm_type, name, address, country_code, housenumber, \
    #     postcode = record.values()
    doc = Doc(record)

    print("osm_id:", doc.osm_id)
    print("osm_type:", doc.osm_type)
    print("name tags as dictionary:", doc.name)
    cursor.close()
    return doc


if __name__ == "__main__":
    connect_and_test_db()

# test_doc()
