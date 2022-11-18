from pymongo import MongoClient, TEXT
import json


# TODO: since the actual MongoImport tool is from the command line, maybe we run the command with our python code?
def mongoimport(jsonfile, db_name, coll_name, db_port):
    port_connection = 'mongodb://localhost:' + str(db_port)
    #print(port_connection)
    client = MongoClient(port_connection)
    db = client[db_name]
    # TODO use drop or delete
    coll = db[coll_name]
    coll.drop()
    print('Dropping table...')
    # print('Deleting entries...')
    # coll.delete_many({})
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
    
    addTokens = {
                "$addFields" : 
                    {
                        "title_tokenized" : {
                            "$split" : ["$title", " "]
                        },
                        "abstract_tokenized" : {
                            "$split" : ["$abstract", " "]
                        },
                        "venue_tokenized" : {
                            "$split" : ["$venue", " "]
                        },
                        "year_string" : {"$toString" : "$year"}
                    }
            }
    addIndex = [('title_tokenized', TEXT), ('year_string', TEXT)]

    
    out = {"$out" : coll_name}
    pipeline = [addTokens, out]
    coll.aggregate(pipeline)
    print('Loaded ' + str(count) + ' entries...')
    coll.create_index(addIndex)        
    return 
#TODO: add text indexs

# ? could we take port number and file name as command line arg which was okayed in forums, could improve productivity?
def main():
    db_port = input('Database port: ')
    # jsonfile = "dblp-ref-1m.json"
    jsonfile = "dblp-ref-1k.json"
    #jsonfile = "dblp-ref-10.json"
    # jsonfile = input('Enter json file name: ')
    db_name = '291db'
    coll_name = 'dblp'
    mongoimport(jsonfile, db_name, coll_name, db_port)


if __name__ == "__main__":
    main()