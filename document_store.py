from pymongo import MongoClient


def connection(port):
    port_connection = 'mongodb://localhost:' + str(port)
    client = MongoClient(port_connection)
    db = client['291db']
    return db

def exit():
    quit()

def main_menu(db):

    while True:
        print('\n\t 1. Search for articles \
        \n\t 2. Search for authors \
        \n\t 3. List the venues \
        \n\t 4. Add an article \
        \n\t 0. Exit \
        \n')
        selection = input('Select from the above options: ')
        if selection == '0':
            exit()
        elif selection == '1':
            searchArticle(db)
        elif selection == '3':
            listVenues(db)
        else:
            print('Please select a valid option...')

def searchArticle(db):
    coll = db['dblp']
    
    
    keywords = input('Enter keywords in a space seperated list: ').lower().split()
    search_field = '{"'
    for each in keywords[:-1]:
        search_field +=  str(each) + '" "'
    search_field += str(keywords[-1]) + '"}'
    

    find = {
        "$text" : 
            {  "$search" : search_field 
                
    }}
    #print(find)

    ret = coll.find(find)
    count = 0
    for each in ret:
        print(str(each))
        count += 1
    print(count)


def searchAuthors(db):
    # provide keyword (SINGULAR)
    # all authors whose name contain the keyword, case insensitive
    # for each author: list name, number of publications
    # user can select author and see title, year, and venue of all articles by that author
    # results should be sorted based on year with recent articles showing up first
    coll = db['dblp']
    return

def listVenues(db):
    coll = db['dblp']
    # n is valid
    while True:
        try:
            n = int(input('Enter an integer to list the top number of venues: '))
        except ValueError:
            print('Please enter a valid integer')
            continue
        else:
            print('Displaying top',n, 'venues...')
            break
    # rest of code
    # for each venue: list venue, number of articles in venue, number of articles that reference paper in venue,
    # sort results based on number of papers that reference that venue
    return

def addArticle(db):
    coll = db['dblp']


    # while True:
        # id = input('Please enter a unique article id: ')
    coll.find_one({{'id':'00638a94-23bf-4fa6-b5ce-40d799c65da7'}})

    # unique id, title, list of authors, year
    # abstract and venue = null
    # references = empty array
    # n_citations = 0
    return 

def main():
    port = input('Enter port number: ')
    db = connection(port)
    main_menu(db)

if __name__ == "__main__":
    main()