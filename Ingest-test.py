from Input import *
from es import *
from tqdm import tqdm
import time
import json
from Address import get_addresses

if __name__ == "__main__":
    connection = connect_to_db()
    index_name = "mydb_suggest"
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


    # doc_count = 3728411
    doc_count = 500000
    a = ""
    print("================================================================")
    sql = "SELECT place_id, osm_id, osm_type, name, address, \
    country_code, housenumber, postcode from placex order by rank_search limit " + str(doc_count)
    print(sql, "\n")

    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(sql)
    records = cursor.fetchall()
    start_time = time.time()
    print("start:", time.time())
    addresses = get_addresses(doc_count)
    data = {}
    for record in records:
        data['address'] = ""
        if record['housenumber']:
            data['address'] += record['housenumber'] + ", "
        data['address'] += addresses[record['place_id']]
        if record['postcode']:
            data['address'] += record['postcode']
        data['osm_type'] = record['osm_type']
        data['osm_id'] = record['osm_id']
        data['place_id'] = record['place_id']
        # print(data)
        place_id, osm_id, osm_type, name, address, country_code, housenumber, \
            postcode = record.values()
        doc = Doc(record)
        header = { "index" : { "_index" : index_name } }
        a += str(json.dumps(header)) + "\n"
        a += str(json.dumps(data)) + "\n"

    print("addresses extracted:", time.time())

    file = open('addresses.op', 'w')
    file.write(a)
    file.close()

    print("Addresses stored in addresses.op and indexing started:", time.time())

    # end_time = time.time()
    # average_time = (end_time - start_time) / doc_count

    # print("For {} docs, Average indexing time is {} seconds per doc.".format(doc_count, average_time))
    # print(a)

    # start_time = time.time()

    elasticsearch.bulk(index=index_name, body=a)

    print("bulk indexing finished:", time.time())

    end_time = time.time()
    average_time = (end_time - start_time) / doc_count

    print("For {} docs, Average indexing time is {}. That is {} seconds per doc.".format(doc_count, end_time - start_time, average_time))
    print(1/average_time)
