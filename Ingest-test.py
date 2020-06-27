from Input import *
from es import *
from tqdm import tqdm
import time
import json

if __name__ == "__main__":
    connection = connect_to_db()
    index_name = "mydb"
    doc_count = 200

    elasticsearch = test_es_connection()
    delete_index(elasticsearch, index_name)
    create_index(elasticsearch, index_name)

    # doc_counts = [10, 100, 500, 1000, 2000]
    doc_counts = []
    for doc_count in doc_counts:
        print("================================================================")
        sql = "SELECT place_id, osm_id, osm_type, name, address, \
        country_code, housenumber, postcode from placex limit " + str(doc_count)
        print(sql, "\n")

        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql)
        records = cursor.fetchall()
        start_time = time.time()
        for record in tqdm(records):
            place_id, osm_id, osm_type, name, address, country_code, housenumber, \
                postcode = record.values()
            doc = Doc(record)
            elasticsearch.index(index_name, doc_type="_doc",
                                body=record)
            # insert_doc(elasticsearch, index_name, record)
            # print(doc.record.values())

        end_time = time.time()
        average_time = (end_time - start_time) / doc_count

        print("For {} docs, Average indexing time is {} seconds per doc.".format(doc_count, average_time))


    doc_counts = [6000]
    a = ""
    for doc_count in doc_counts:
        print("================================================================")
        sql = "SELECT place_id, osm_id, osm_type, name, address, \
        country_code, housenumber, postcode from placex limit " + str(doc_count)
        print(sql, "\n")

        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql)
        records = cursor.fetchall()
        start_time = time.time()
        for record in records:
            place_id, osm_id, osm_type, name, address, country_code, housenumber, \
                postcode = record.values()
            doc = Doc(record)
            header = { "index" : { "_index" : index_name } }
            a += str(json.dumps(header)) + "\n"
            a += str(json.dumps(record)) + "\n"
            # elasticsearch.index(index_name, doc_type="_doc",
                                # body=record)
            # insert_doc(elasticsearch, index_name, record)
            # print(doc.record.values())

        end_time = time.time()
        average_time = (end_time - start_time) / doc_count

        print("For {} docs, Average indexing time is {} seconds per doc.".format(doc_count, average_time))
    # print(a)

    # start_time = time.time()
    
    elasticsearch.bulk(index=index_name, body=a)
    
    end_time = time.time()
    average_time = (end_time - start_time) / doc_count

    print("For {} docs, Average indexing time is {}. That is {} seconds per doc.".format(doc_count, end_time - start_time, average_time))
    print(1/average_time)
