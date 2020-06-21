from Input import *
# from es import *
from tqdm import tqdm
import time


if __name__ == "__main__":
    connection = connect_to_db()

    doc_counts = [200]
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
            if(placex_record['name']):
                print(placex_record['name']['name'], ', ', sep="", end="")
            flag = True
            i = 1
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
                print(placex_record["name"]['name'], ',', sep="", end="")
            print(placex_record['country_code'])
