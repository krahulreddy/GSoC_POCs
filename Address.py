from Input import *
# from es import *
from tqdm import tqdm
import time
import pprint


# if __name__ == "__main__":
def get_addresses(count):
    connection = connect_to_db()

    addresses = {}

    doc_counts = []
    for doc_count in doc_counts:
        print("================================================================")
        print("================================================================")
        sql = "SELECT * from place_addressline where isaddress=true"
        print(sql, "\n")

        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql)

        records = cursor.fetchall()
        start_time = time.time()

        for p_record in records:
            # print("================================================================")
            sql = "SELECT place_id, name, address, country_code, housenumber, postcode from placex where place_id = " + \
                str(p_record['place_id'])
            # print(sql, "\n")
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql)

            placex_record = cursor.fetchone()
            # print(placex_record)
            if placex_record['name'] and "name" in placex_record["name"]:
                add = placex_record['name']['name'] + ', '
                print(placex_record['name']['name'], ', ', sep="", end="")
            flag = True
            i = 1
            add = ""
            while flag:
                # print("**" * i)
                i += 1

                sql = "SELECT * from place_addressline where isaddress=true and place_id = " + str(p_record['address_place_id'])
                # print(sql, "\n")

                cursor = connection.cursor(cursor_factory=RealDictCursor)
                cursor.execute(sql)

                p_record = cursor.fetchone()
                # print(p_record)

                if not p_record:
                    # print("")
                    break

                sql = "SELECT place_id, name, address, country_code, housenumber, postcode from placex where place_id = " + \
                    str(p_record['place_id'])
                # print(sql, "\n")
                cursor = connection.cursor(cursor_factory=RealDictCursor)
                cursor.execute(sql)

                placex_record = cursor.fetchone()
                if placex_record["name"] and placex_record["name"]['name']:
                    add += placex_record["name"]['name'] +  ', '
                # print(placex_record["name"]['name'], ',', sep="", end="")
            add += placex_record['country_code']
            # print(placex_record['country_code'])
            print(add)

            addresses[placex_record["place_id"]] = add

    addresses = {}
    doc_count = count
    # for doc_count in doc_counts:

    print("================================================================")
    print("================================================================")
    sql = "SELECT place_id, parent_place_id, name, address, country_code, housenumber, postcode, rank_search, rank_address from placex order by rank_search limit " + str(doc_count)
    print(sql, "\n")

    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(sql)

    records = cursor.fetchall()
    start_time = time.time()

    for p_record in tqdm(records):
        # print("================================================================")
        # print(p_record)
        if p_record["rank_address"] > 37:
            print(">27")
            print(p_record)
            if p_record['parent_place_id'] in addresses:
                print("Address is ", addresses[p_record['parent_place_id']])
                add = addresses[p_record['parent_place_id']] + "' "
            # continue
        sql = "SELECT * from place_addressline where isaddress=true and place_id = " + str(p_record['place_id'])

        # print(sql, "\n")
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql)

        a_record = cursor.fetchone()
        try:
            add = p_record['name']['name'] + ", "
        except:
            add = ""
        # print(a_record)

        if not a_record:
            addresses[p_record['place_id']] = add
            
        flag = (a_record != None)
        # print(flag, a_record)
        # sql = "SELECT place_id, name, address, country_code, housenumber, postcode from placex where place_id = " + \
        #     str(p_record['place_id'])
        # print(sql, "\n")
        # cursor = connection.cursor(cursor_factory=RealDictCursor)
        # cursor.execute(sql)

        # placex_record = cursor.fetchone()
        # # print(placex_record)
        # if placex_record['name'] and "name" in placex_record["name"]:
        #     add = placex_record['name']['name'] + ', '
        #     print(placex_record['name']['name'], ', ', sep="", end="")

        i = 1
        while flag:
            if i > 2:
                print("**" * i)
            i += 1

            # print(a_record)
            sql = "SELECT * from place_addressline where isaddress=true and place_id = " + str(a_record['address_place_id'])
            # print(sql, "\n")

            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql)

            a_record = cursor.fetchone()
            # print(p_record)

            if not a_record:
                # print("")
                break

            sql = "SELECT place_id, name, address, country_code, housenumber, postcode from placex where place_id = " + \
                str(a_record['place_id'])
            # print(sql, "\n")
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql)

            placex_record = cursor.fetchone()
            # if placex_record["name"] and placex_record["name"]['name']:
            try:
                add += placex_record["name"]['name'] +  ', '
            except:
                add += str(placex_record["name"])
                # print(add)

            # print(placex_record["name"]['name'], ',', sep="", end="")
        try:
            add += addresses[p_record['parent_place_id']]
            # print(add)
        except:
            pass
        # add += p_record['country_code']
        # print(placex_record['country_code'])
        if i > 3:
            print(add)
        addresses[p_record["place_id"]] = add
        
    # pprint.pprint(addresses)
    return addresses