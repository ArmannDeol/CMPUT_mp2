from pymongo import MongoClient
import json

def mongoimport(jsonfile, db_name, coll_name, db_port):

    port_connection = 'monogdb://localhost:' + str(db_port)
    client = MongoClient(port_connection)
    db = client[db_name]
    coll = db[coll_name]
    coll.delete_many({})
    count = 0
    with open(jsonfile) as file:
        line = file.readline()
        print("Running...")
        
        while line:
            line = json.loads(line)
            #print(line)
            coll.insert_one(line)
            line = file.readline()
            count += 1

    print('Loaded ' + count + ' entries...')        
    return 


def main():
    db_port = input('Database port: ')
    
    jsonfile = "dblp-ref-1m.json"
    jsonfile = input('Enter json file name: ')
    db_name = '291_db'
    coll_name = 'dblp'
    mongoimport(jsonfile, db_name, coll_name, db_port)


if __name__ == "__main__":
    main()