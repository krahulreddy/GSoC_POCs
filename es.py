from elasticsearch import Elasticsearch
from urllib.request import urlopen
import json

elasticsearch = Elasticsearch()
index_name = "nominatim_test"


def test_es_connection():
    try:
        print(elasticsearch.ping())
        print(elasticsearch.info())
    except:
        print("Failed. Start the elasticsearch server and try again.")
        exit


def create_index():
    print("=================================================================")
    print("Trying to create an index")
    try:
        print("Checking if the index exists")
        if elasticsearch.indices.exists(index_name):
            print("Index", index_name, "exists")
        else:
            print("Index does not exist. Creating index", index_name)
            elasticsearch.indices.create(index_name)
    except:
        print("Failed")


def insert_doc():
    print("=================================================================")
    print("Trying to insert doc into", index_name)
    try:
        doc = {"age": 21, "first name": "Rahul", "last name": "Reddy"}
        elasticsearch.index(index_name, doc_type="_doc",
                            body=doc)
        print("Indexed " + str(doc) + " successfully")
    except:
        print("Failed")

def search_results():
    print("=================================================================")
    print("Trying to search for `Rahul` from", index_name)
    try:
        print(elasticsearch.search(index_name, q="Rahul"))
        # elasticsearch.index(index_name, doc_type="_doc",
        #                     body={"age": 21, "first name": "Rahul", "last name": "Reddy"})
        # print("Added document successfully")
    except:
        print("Failed")

def delete_index():
    print("=================================================================")
    print("Trying to delete an index")
    try:
        print("Checking if the index exists")
        if elasticsearch.indices.exists(index_name):
            print("Index", index_name, "exists")
            elasticsearch.indices.delete(index_name)
            print("Index successfully deleted")
        else:
            print("Index does not exist.", index_name)
    except:
        print("Failed")


if __name__ == "__main__":
    test_es_connection()
    create_index()
    insert_doc()
    search_results()
    delete_index()
