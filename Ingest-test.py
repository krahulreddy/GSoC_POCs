from Input import *
from es import *
from tqdm import tqdm
import time
import json
from Address import get_addresses
import matplotlib.pyplot as plot

if __name__ == "__main__":
    # q = [3454.2600817228667, 2812.3241979512472, 2444.1334868381496, 2167.952715230194, 1954.2042321785586, 1796.5412929464228, 1668.133668115771, 1562.518195926698, 1464.851426800792, 1379.8338793347477, 1245.3696831603415, 1176.1078295221369, 1122.019039542533, 1077.5333933347565, 1032.9225239644545, 990.6500654504395, 955.2676908649611, 921.8829740157494, 890.1973624704898, 824.9980281340523, 800.9714464540749, 776.8661275214771, 754.4226197586478, 734.3056621085161, 714.1898655464109, 694.6617373687051, 676.0825358203225, 660.2768989427555, 626.9157464987966, 609.7762812801401, 595.2720203323173, 582.2221930792905, 570.1255024554976, 557.7431241147195, 544.7246190808808, 533.3501931327836, 522.5143495333691, 512.1603845999308, 501.2608541001816, 492.1987025965314]
    # plot.plot(range(1000, 80000, 2000), q)
    plot.show()
    connection = connect_to_db()
    index_name = "mydb"
    doc_count = 200
    # a = 0/0
    # print(A)
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


    doc_count = 100000
        # th = 40000
    print("================================================================")
    sql = "SELECT place_id, osm_id, osm_type, name, address, \
    country_code, housenumber, postcode from placex order by rank_search limit " + str(doc_count)
    print(sql, "\n")

    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(sql)
    records = cursor.fetchall()
    addresses = get_addresses(doc_count)
    avg_rates = []
    ths = range(1000, 100000, 5000)
    for th in ths:
        print("================================================================")
        print("th =", th)
        print("================================================================")
        start_time = time.time()

        delete_index(elasticsearch, index_name)
        create_index(elasticsearch, index_name)

        a = ""
        data = {}
        bulk_index_times = []
        for i, record in enumerate(records):
            if i % th == 0 and i != 0:
                t1 = time.time()
                elasticsearch.bulk(index=index_name, body=a)
                # print("Time for {}th bulk indexing is {}".format(i, time.time() - t1))
                bulk_index_times.append(time.time() - t1)
                a = ""
                data = {}
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

        # end_time = time.time()
        # average_time = (end_time - start_time) / doc_count

        # print("For {} docs, Average indexing time is {} seconds per doc.".format(doc_count, average_time))
        # print(a)

        # start_time = time.time()
        t1 = time.time()
        elasticsearch.bulk(index=index_name, body=a)
        bulk_index_times.append(time.time() - t1)

        end_time = time.time()
        average_time = (end_time - start_time) / doc_count

        avg_rates.append(1/average_time)

        print("For {} docs, Average indexing time is {}. That is {} docs per second.".format(doc_count, end_time - start_time, 1/average_time))
        print("Bulk index operation total time:", sum(bulk_index_times))
        print("Bulk index operation average time:", sum(bulk_index_times) / len(bulk_index_times))
        
    print(avg_rates)
    plot.plot(ths, avg_rates)
    plot.show()
