from pymongo import MongoClient
import pandas as pd
import os
import ast

import json

def mongoimport(csv_path, db_name, coll_name, db_url='localhost', db_port=27000):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    client = MongoClient('mongodb://localhost:16639')
    db = client[db_name]
    coll = db[coll_name]
    coll.delete_many({})
    with open(csv_path) as file:
        line = file.readline().strip()
        
        #file_data = json.load(file)
        while line:
            content = ast.literal_eval(line)
            
            print(content)
            coll.insert_one(content)
            line = file.readline().strip()
            
        
    
    return #coll.count()


def main():
    db_port = input('Database port: ')
    db_url = 'localhost'
    csv_path = "dblp-ref-1m.json"
    db_name = '291_db'
    coll_name = 'dblp'
    mongoimport(csv_path, db_name, coll_name,db_url, db_port)


if __name__ == "__main__":
    main()