from sys import argv
from pymongo import MongoClient, TEXT, ASCENDING, DESCENDING
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
    coll.drop()
    print('Dropping table...')

    count = 0
    with open(jsonfile) as file:
        line = file.readline()
        print("Running...")
        
        while line:
            line = json.loads(line)
            coll.insert_one(line)
            line = file.readline()
            count += 1
    
    addIndex = [('title', TEXT), ('year', TEXT), ('abstract', TEXT), ('venue', TEXT), ('authors', TEXT)]
    print('Loaded ' + str(count) + ' entries...')
    print('Creating Indexes...')
    coll.create_index(addIndex)      
    return 

def main():
    if (len(argv)) > 1:
        db_port = str(argv[1])
        jsonfile = str(argv[2])
    else:
        db_port = input('Database port: ')
        #jsonfile = input('Enter json file name: ')
    #jsonfile = "dblp-ref-1m.json"
    jsonfile = "dblp-ref-1k.json"
    # jsonfile = "dblp-ref-10.json"
    db_name = '291db'
    coll_name = 'dblp'
    mongoimport(jsonfile, db_name, coll_name, db_port)

if __name__ == "__main__":
    main()