from pymongo import MongoClient, TEXT, ASCENDING
from sys import argv
from subprocess import run


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
    # importing documents with MongoImport
    run(['mongoimport', '-d', db_name, '-c', coll_name, '--file', jsonfile, '--port', db_port, '--drop', '--batchSize', '1000'])
    # creating collection indexes
    addIndex = [
        ('abstract', TEXT),
        ('authors', TEXT),
        ('title', TEXT),
        ('venue', TEXT),
        ('year', TEXT)
    ]
    coll.create_index(addIndex)
    coll.create_index([('references', ASCENDING)])
    # precomputing for view used in listVenues, does not work with 1m file
    if jsonfile != 'dblp-ref-1m.json':
        venueInfoAgg = [
            {
                '$lookup' :
                    {
                        'from' : 'dblp',
                        'localField': 'id',
                        'foreignField': 'references',
                        'as' : 'ids_ref_art'
                    }
            },
            {'$unwind' : '$ids_ref_art'},
            {
                '$group' : {'_id' : '$venue', 'articles_in_venue' : {'$addToSet' : '$id'}, 'unique_ids' : {'$addToSet' : '$ids_ref_art'}}
            },
            {'$project' : {'venue' : 1, 'num_articles_refs_venue': {'$size' : '$unique_ids'}, 'num_articles_in_venue' : {'$sum' : 1}}},
            {'$merge' : {'into': 'venueInfo'}}
        ]
        collVenues = db['venueInfo']
        collVenues.drop()
        coll.aggregate(venueInfoAgg)
    return 

def main():
    if (len(argv)) > 2:
        db_port = str(argv[1])
        jsonfile = str(argv[2])
    else:
        print('Please enter <port> and <file-name> as args')
        quit()
    db_name = '291db'
    coll_name = 'dblp'
    mongoimport(jsonfile, db_name, coll_name, db_port)

if __name__ == "__main__":
    main()