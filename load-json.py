from sys import argv
from pymongo import MongoClient, TEXT, ASCENDING, DESCENDING
import json


def mongoimport(jsonfile, db_name, coll_name, db_port):
    """_summary_

    Args:
        jsonfile (_type_): _description_
        db_name (_type_): _description_
        coll_name (_type_): _description_
        db_port (_type_): _description_
    """
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
                            "$map" : {
                            "input" : {"$split" : ["$title", " "]},
                            "as" : "split_title",
                            "in" : {"$toLower" : "$$split_title"} 
                        }},
                        "abstract_tokenized" : {
                            "$map" : {
                            "input" : {"$split" : ["$abstract", " "]},
                            "as" : "split_abstract",
                            "in" : {"$toLower" : "$$split_abstract"} 
                        }},
                        "venue_tokenized" : {
                            "$map" : {
                            "input" : {"$split" : ["$venue", " "]},
                            "as" : "split_venue",
                            "in" : {"$toLower" : "$$split_venue"} 
                        }},
                        "authors_lower" : {
                            "$map" : {
                            
                                "input" : "$authors",
                                "as" : "authors",
                                "in": {"$toLower" : "$$authors"}
                        }},
                        "year_string" : {"$split" : [
                        {"$toString" : "$year"}, " "]}, 
                    }
            }
    addIndex = [('title_tokenized', TEXT), ('year_string', TEXT), ('abstract_lower', TEXT), ('venue_lower', TEXT), ('year_string', TEXT), ('year', ASCENDING)]

    
    out = {"$out" : coll_name}
    pipeline = [addTokens, out]
    coll.aggregate(pipeline)
    addConcatenation = {"$addFields" : {
            "concatenated" : {"$concatArrays" : 
            [{"$ifNull" : ["$title_tokenized", []]}, 
            {"$ifNull" : ["$abstract_tokenized", []]}, 
            {"$ifNull" : ["$venue_tokenized", []]},
            {"$ifNull" : ["$authors_lower", []]},
            {"$ifNull" : ["$year_string", []]}]}
            }
        }
    pipeline = [addConcatenation, out]
    coll.aggregate(pipeline)
    
    print('Loaded ' + str(count) + ' entries...')

    coll.create_index(addIndex)        
    return 



# ? added command line argument input because helps with testing
def main():
    # print(argv)
    if (len(argv)) > 1:
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