import unittest
import sys
import document_store as ds
from subprocess import run
from unittest.mock import patch


class TestName(unittest.TestCase):
    def setUp(self):
        port = '2000'
        result = run(['python3', 'load-json.py', port, 'dblp-ref-10.json'])
        self.db = ds.connection(port)

    def test_addArticle(self):
        # ds.addArticle(self.db)
        print('test1')
        return

if __name__ == '__main__':
    unittest.main()