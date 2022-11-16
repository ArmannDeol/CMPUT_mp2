from pymongo import MongoClient
import json


# TODO: rename file to load-json.py as per spec
# TODO: since the actual MongoImport tool is from the command line, maybe we run the command with our python code?
def mongoimport(jsonfile, db_name, coll_name, db_port):

    port_connection = 'mongodb://localhost:' + str(db_port)
    #print(port_connection)
    client = MongoClient(port_connection)
    db = client[db_name]
    # TODO: spec states to drop the table, "col.drop"? Not sure if there is a performance or big diff between the two
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
                        }
                    }
            }
    out = {"$out" : coll_name}
    pipeline = [addTokens, out]
    coll.aggregate(pipeline)
    print('Loaded ' + str(count) + ' entries...')        
    return 
#TODO: tokenize title, abstract, venue

def main():
    db_port = input('Database port: ')
    jsonfile = "dblp-ref-1m.json"
    jsonfile = input('Enter json file name: ')
    db_name = '291db'
    coll_name = 'dblp'
    mongoimport(jsonfile, db_name, coll_name, db_port)


if __name__ == "__main__":
    main()