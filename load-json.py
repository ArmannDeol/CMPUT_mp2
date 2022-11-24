from sys import argv
from os import system
from pymongo import MongoClient, TEXT, ASCENDING, DESCENDING
from subprocess import run, DEVNULL
import json


def mongoimport(jsonfile, db_name, coll_name, db_port):
    """
    Imports JSON file from file and inserts values into a mongodb
    database

            Parameters:
                jsonfile (String): JSON file name
                db_name (String): Database name
                coll_name (String): Collection name
                db_port (String): Port number
            Returns:
                None
    """
    port_connection = 'mongodb://localhost:' + str(db_port)
    client = MongoClient(port_connection)
    db = client[db_name]
    coll = db[coll_name]
    run(['mongoimport', '-d', db_name, '-c', coll_name, '--file', jsonfile, '--port', db_port, '--drop'])
    # TODO DELETE
    # count = 0
    # with open(jsonfile) as file:
    #     line = file.readline()
    #     # print("Running...")
        
    #     while line:
    #         line = json.loads(line)
    #         coll.insert_one(line)
    #         line = file.readline()
    #         count += 1
    # materialized view with venues and number of articles in venue
    # matViewAgg = [
    #     # need venue, num, and id
    #     {
    #         '$group' : {'_id' : '$venue', 'num_articles_in_venue' : {"$sum" : 1}}
    #     },
    #     {
    #         '$project' : {'venue' : '$_id', '_id' : 0, 'num_articles_in_venue' : 1, 'id' : 1}
    #     },
    #     {
    #         '$merge' : {'into': 'venueInfo'}
    #     }
    # ]
    collVenues = db['venueInfo']
    collVenues.drop()
    matViewAgg = [
        {
            '$project' : {'venue' : 1, 'id' : 1}
        },
        {
            '$merge' : {'into': 'venueInfo'}
        }
    ]
    coll.aggregate(matViewAgg)
    # TODO DELETE
    matches = collVenues.find()
    # for e in matches:
    #     print(e)
    # addIndex = [('title', TEXT), ('year', TEXT), ('abstract', TEXT), ('venue', TEXT), ('authors', TEXT)]
    addIndex = [
        ('abstract', TEXT),
        ('authors', TEXT),
        ('title', TEXT),
        ('venue', TEXT),
        ('year', TEXT)
    ]
    addRefIndex = [('references', DESCENDING)]
    # print('Creating Indexes...')
    coll.create_index(addIndex)
    coll.create_index(addRefIndex)
    # matches = coll.list_indexes()
    # for e in matches:
    #     print(e)
    return 

def main():
    if (len(argv)) > 2:
        db_port = str(argv[1])
        jsonfile = str(argv[2])
    else:
        db_port = input('Database port: ')
        jsonfile = input('Enter json file name: ')
    # jsonfile = "dblp-ref-1m.json"
    # jsonfile = "dblp-ref-1k.json"
    # jsonfile = "dblp-ref-10.json"
    db_name = '291db'
    coll_name = 'dblp'
    mongoimport(jsonfile, db_name, coll_name, db_port)

if __name__ == "__main__":
    main()