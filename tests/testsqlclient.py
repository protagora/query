from google.cloud import bigquery
import json
import os
import sys
import unittest

sys.path.append(os.path.dirname(os.getcwd()))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    "credentials",
    "credentials.json"
)

from sqlbuilder import Composer
from sqlclient import SQLClient

class ConfigurationStub():
    EXPECTED_RESULT = 'EXPECTED RESULT'
    EXPECTED_QUERY = 'EXPECTED QUERY'

    ERROR_ACCESS = "Can not open file: {}"
    ERROR_FORMAT = "Not a valid JSON format: {}"

    def __init__(self, path):
        self._path = None
        self._config = None
        self._setup(path)

    def expected_result(self, tuples=True):
        if tuples:
            output = []
            for item in self.config[self.EXPECTED_RESULT]:
                output.append(tuple(item))
        else:
            output = self.config[self.EXPECTED_RESULT]
        return output

    def expected_query(self):
        return self.config[self.EXPECTED_QUERY]

    @property
    def path(self):
        return self._path
    @path.setter
    def path(self, path):
        self._path = str(path)

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self, config):
        self._config = config

    def _setup(self, path):
        self.path = path
        self._load()

    def _load(self):
        try:
            with open(self.path) as handle:
                data = handle.read()
        except:
            raise Exception(self.ERROR_ACCESS.format(self.path))
        try:
            self.config = json.loads(data)
        except:
            raise Exception(self.ERROR_FORMAT.format(self.path))

    def __call__(self):
        return self.config

    def __repr__(self):
        return self.path

    def __str__(self):
        return self.path

class TestClientInstance(unittest.TestCase):

    def test_instance(self):
        client = SQLClient()
        self.assertIsInstance(client, SQLClient)
        self.assertIsInstance(client.client, bigquery.Client)

    def test_sqlquery(self):
        query = "Test Sql Query String"
        client = SQLClient()
        try:
            client.query(query)
        except:
            pass
        self.assertEqual(client.sqlquery, query)

class TestUseCases(unittest.TestCase):

    def setUp(self):
        self.client = SQLClient()

    def test_TestCase3(self):
        TEST_CASE = os.path.join('config', 'testCase3.json')
        config = ConfigurationStub(TEST_CASE)
        composer = Composer(TEST_CASE)
        expected_query = config.expected_query()
        expected_result = config.expected_result()

        query = composer.buildQuery()
        self.client.query(query)
        result = self.client.fetchall()

        self.assertEqual(query, expected_query)
        self.assertEqual(result, expected_result)

if "__main__" == __name__:
    unittest.main()
