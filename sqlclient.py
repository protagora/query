from google.cloud import bigquery
import logging
import traceback

logger = logging.getLogger('SQLClient')

class SQLClient(object):
    def __init__(self):
        self._client = None
        self._sqlquery = None
        self._setup()

    @property
    def client(self):
        return self._client
    @client.setter
    def client(self, client):
        self._client = client

    @property
    def result(self):
        return self._result
    @result.setter
    def result(self, result):
        self._result = result

    @property
    def records(self):
        return self._records
    @records.setter
    def records(self, records):
        self._records = records

    @property
    def sqlquery(self):
        return self._sqlquery
    @sqlquery.setter
    def sqlquery(self, sqlquery):
        self._sqlquery = sqlquery

    def _setup(self, clientInterface=None):
        if clientInterface is None:
            self.client = bigquery.Client()
        else:
            self.client = clientInterface

    def _execute(self):
        self.result = self.client.query(self.sqlquery)
        self.records = [item.values() for item in self.result.result()]

    def query(self, sqlQuery):
        self.sqlquery = sqlQuery
        try:
            self._execute()
        except:
            logger.info("Query error!\nQuery:\n`{}`\nReason: {}".format(str(self.sqlquery), traceback.format_exc()))
            raise Exception("Query error!\nQuery:\n`{}`\nReason: {}".format(str(self.sqlquery), traceback.format_exc()))

    def fetchall(self):
        return self.records

if "__main__" == __name__:
    print("SQLClient is a package file, execution has no effects.\nTo execute tests suite run testsqlclient.py")
