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
        print('\n\t\t Main Menu \
        \n\t 1. Search for articles \
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
        elif selection == '2':
            searchAuthors(db)
        elif selection == '3':
            listVenues(db)
        else:
            print('Please select a valid option...')

def searchArticle(db):
    coll = db['dblp']
    
    keywords = input('Enter keywords in a space seperated list: ').lower().split()
    if keywords == []:
        return
    search_field = '{"'
    for each in keywords[:-1]:
        search_field +=  str(each) + '" "'
    search_field += str(keywords[-1]) + '"}'
    

    find = {
        "$text" : 
            {  "$search" : search_field 
                
    }}
    #print(find)

    matches = coll.find(find)
    matches2 = matches.clone()
    index = 0
    info = []    
    for each in matches:
        option = ''
        if each['id'] == '':
            option += 'None || '
        else:
            option += str(each['id']) + ' || '
        
        if each['title'] == '':
            option += 'None || '
        else:
            option += str(each['title']) + ' || '

        if each['year'] == '':
            option += 'None || '
        else:
            option += str(each['year']) + ' || '
 
        if each['venue'] == '':
            option += 'None'
        else:
            option += str(each['venue'])

        info.insert(index, option)
        index += 1
    header = '\t ID || Title || Year || Venue'
    selection = paginate(info, header)

    if selection != None:
        output = matches2.__getitem__(selection)
        if 'id' in output:
           print('\nID: ' + str(output['id']))
        else:
            print('\nID: None')
        
        if 'title' in output:
           print('Title: ' + str(output['title']))
        else:
            print('Title: None')

        if 'year' in output:
           print('Year: ' + str(output['year']))
        else:
            print('Year: None')
        
        if 'venue' in output:
           print('Venue: ' + str(output['venue']))
        else:
            print('Venue: None')
        
        if 'abstract' in output:
           print('Abstract: ' + str(output['abstract']))
        else:
            print('Abstract: None')
        
        if 'authors' in output:
            print('Author(s): ' + output['authors'][0])
            for each in output['authors'][1:]:
                print('\t  ' + str(each))
        else:
            print('Authors: None')

        referenced = coll.find({"references" : output["id"]})
        referenced_count = coll.count_documents({"references" : output["id"]})
        if referenced_count == 0:
            print('Referenced by None')
        else: 
            print('Referenced by: ')
            for each in referenced:
                if 'id' in each:
                    print('\nID: ' + str(output['id']))
                else:
                    print('\nID: None')

                if 'title' in each:
                    print('Title: ' + str(output['title']))
                else:
                    print('Title: None')

                if 'year' in each:
                    print('Year: ' + str(output['year']))
                else:
                    print('Year: None')
    return

def searchAuthors(db):
    # provide keyword (SINGULAR)
    # all authors whose name contain the keyword, case insensitive
    # for each author: list name, number of publications
    # user can select author and see title, year, and venue of all articles by that author
    # results should be sorted based on year with recent articles showing up first
    coll = db['dblp']
    keyword = input('Enter keyword: ').lower()
    find = {
        "$text" : 
            {  "$search" : keyword 
                
    }}
    matches_unfiltered = coll.distinct("authors", find)
    matches = []
    info = []
    index = 0
    for each in matches_unfiltered:
        if keyword in str(each).lower():
            matches.insert(index, each)
            
            count = coll.count_documents({"authors" : each})
            option = str(each) + ' || ' + str(count)
            info.insert(index, option)
            index += 1
    header = '\tArtist Name || Number of Publications'
    selection = paginate(info, header)
          
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

def paginate(info, header, label = None):
    
    amount = 1
    paginated = []
    change = True
    i = 0

    while True:
        if label != None:
            print('\t\t ' + label)
        else:
            print('\t\t Search Results')

        print(header)
        while amount <= len(info) and i < 5 and change == True:
            paginated.insert(i, info[amount-1])
            i += 1
            amount += 1
        index = 0
        change = True
        for each in paginated:
            index += 1
            print('\t' + str(index) + '. ' + str(each))
            
        print('\t6. Page Up \n\t7. Page Down \n\t0. Back')
        selection = input('\nSelect from the above options: ')
        if selection.strip() == '0':
            return
        elif selection.strip() == '6':
            amount = amount - index - 5
            paginated = []
            i = 0
            if amount <= 0:
                amount = 1
        elif selection.strip() == '7':
            if amount <= len(info):
                paginated = []
                i = 0
            continue
        elif selection.strip() == '1':
            change = False
            return(amount-i-1)
                
                
        elif selection.strip() == '2':
            change = False 
            if i >= 2:
                return(amount-i)
                    
            else:
                print('Please select a valid option...')

        elif selection.strip() == '3':
            change = False
            if i >= 3:
                return(amount-i+1)
            else:
                print('Please select a valid option...')

        elif selection.strip() == '4':
            change = False
            if i >= 4:
                return(amount-i+2)
                    
            else:
                print('Please select a valid option...')

        elif selection.strip() == '5':
            change = False
            if i >= 5:
                return(amount-i+3)
            else:
                print('Please select a valid option...')

        else:
            print('Please enter a valid option...')
            change = False


def main():
    port = input('Enter port number: ')
    db = connection(port)
    main_menu(db)

if __name__ == "__main__":
    main()