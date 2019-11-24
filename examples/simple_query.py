import os
import sys

sys.path.append(os.path.dirname(os.getcwd()))

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    "credentials",
    "credentials.json"
)

from sqlbuilder import *
from sqlclient import SQLClient

'''
Simple query example

Loads simple query definition,
creates client instance,
builds SQL query string,
executes query and gets all results
'''

query_definition_path = os.path.join('input', 'simple.json')
composer = Composer(query_definition_path)
client = SQLClient()

print('Building query...')
query = composer.buildQuery()

print('Running query remotely...')
client.query(query)

print('Retrieving result set..')
result = client.fetchall()

'''
Output
'''
print('\n')
print('Constructed SQL query:')
print(query)

print('\n')
print('Result set:')
for item in result:
    print(item)
print('\n')
