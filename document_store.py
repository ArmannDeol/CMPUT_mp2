from pymongo import MongoClient


def connection(port):
    port_connection = 'mongodb://localhost:' + str(port)
    client = MongoClient(port_connection)
    db = client['291db']
    return db

def main_menu(db):

    while True:
        print('\n\t 1. Search for articles \
        \n\t 2. Search for authors \
        \n\t 3. List the venues \
        \n\t 4. Add an article \
        \n\t 0. Exit \
        \n')
        selection = input('Select from the above options: ')

        if selection == '1':
            searchArticle(db)

def searchArticle(db):
    coll = db['dblp']
    keywords = input('Enter keywords in a space seperated list: ').split()
    lower = {
        "$project":
            {
                "title" : {"$toLower" : "$title"},
                "authors" : {"$toLower" : "$authors"},
                "abstract" : {"$toLower" : "$abstract"},
                "venue" : {"$toLower" : "$venue"},
                "year" : {"$toLower" : "$year"}
            }
    }
    #year = int(keywords[0])
    matching = {
        "$match" :
            {
                "title" : 
                    {
                        "$regex" : keywords[0]
                    }
                    
            }
    }
    limit = {"$limit" : 5}
    

    pipeline = [matching, limit]
    ret = coll.aggregate(pipeline)
    # ret_count = coll.count_documents({"year" : 2011})
    # print(ret_count)
    for each in ret:
        print(each["title"])




def main():
    port = input('Enter port number: ')
    db = connection(port)
    main_menu(db)

if __name__ == "__main__":
    main()