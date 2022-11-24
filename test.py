import unittest
import builtins
import os
import sys
import document_store as ds
from io import StringIO
from subprocess import run, DEVNULL
from unittest.mock import patch




class TestName(unittest.TestCase):
    # def setUp(self):
    #     port = '2000'
    #     run(['python3', 'load-json.py', port, 'dblp-ref-10.json'])
    #     self.db = ds.connection(port)

    # @patch('document_store.input', create=True)
    # def test_addArticle_non_uniqueID(self, mock_input):
    #     # imitates user input
    #     mock_input.side_effect = ['0040b022-1472-4f70-a753-74832df65266', '50']
    #     ds.addArticle(self.db)
        
    # @patch('document_store.input', create=True)
    # def test_addArticle_uniqueID(self, mock_input):
    #     port = '2000'
    #     run(['python3', 'load-json.py', port, 'dblp-ref-10.json'])
    #     self.db = ds.connection(port)
    #     id = '50'
    #     # imitates user input
    #     mock_input.side_effect = [id, 'FunnyTitle', 'Joe', 'Moe', '-1', '2000']
    #     ds.addArticle(self.db)
    #     coll = self.db['dblp']
    #     matches = coll.find_one({'id' : id})
    #     self.assertIsNotNone(matches)
    
    @patch('document_store.input', create=True)
    def test_listVenues(self, mock_input):
        port = '2000'
        run(['python3', 'load-json.py', port, 'dblp-test.json'])
        self.db = ds.connection(port)
        
        n = 3
        # imitates user input
        mock_input.side_effect = [n]
        ds.listVenues(self.db)

if __name__ == '__main__':
    unittest.main()