#!/usr/bin/env python
import unittest
from query import Agg

from google.cloud import bigquery
import os
import time
os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "gcs_testing.json"

# ** In Result **
# - Needs PIVOT?
# - TOTAL_X_LIMIT_REACHED = False
# - TOTAL_Y_LIMIT_REACHED = False
# - AGGREGATION_LIMIT_REACHED = ['field1', 'field2']

# (1) microsecond precision == 6 digits for TIME,DATETIME,NUMBER
# (2) storing duration|time|datetime in Perspective
# (3) storing array in pespective
# (4) storing boolean in perspective, etc. (true / false; True / False; yes / no; Yes / No; y / n; Y / N; on / off; 1 / 0.)

# In the return results, how do we know if NEEDS_PIVOT? or not
# Geo data types:
# Geo-point: Geo_point for latitude/longitude points
# Geo-Shape: Geo_shape for complex shapes such as polygons

TEST_CASES = [
    
    {
        
        # (1) TABLE_NAME
        'TABLE_NAME': "datadocs-163219.010ff92f6a62438aa47c10005fe98fc9.inv",
        
        # (2) GROUP_BY_X
        # Possible Types to GroupBy: String, Number, Date
        'GROUP_BY_X': [
            {
                "Field": "category",
                "Limit": 5,
                
                # We want to allow sorting by any value, or itself (such as "Category A-Z")
                # If easier, we could do a sort as an Index --> 0 meaning VALUES[0], 1 meaning VALUES[1], etc.
                "Sort": "COUNT(round)",
                "SortDirection": "DESC",
                
                # Can be one of YEAR|QUARTER|MONTH|WEEK|DAY|HOUR
                "DateAggregation": None,
            },
            
        ],
        
        
        # (3) VALUES (only applied if a GROUP_BY_X or GROUP_BY_Y is applied, short for "Aggregation Value")
        # Possible Operations by Field Type:
        # STRING -- ANY:STRING | COUNT:NUMBER | DISTINCT_COUNT:NUMBER | VALUES:ARRAY | DISTINCT_VALUES: ARRAY | LENGTH_VALUES:NUMBER | LENGTH_DISTINCT_VALUES: NUMBER
        # NUMBER -- ANY:STRING | COUNT:NUMBER | DISTINCT_COUNT:NUMBER | AVERAGE:NUMBER | MIN:NUMBER | MAX: NUMBER | MEAN:NUMBER | SUM:NUMBER | VALUES:ARRAY | DISTINCT_VALUES: ARRAY | LENGTH_VALUES:NUMBER | LENGTH_DISTINCT_VALUES: NUMBER
        # DATE --   ANY:STRING | COUNT:NUMBER | DISTINCT_COUNT:NUMBER | VALUES:ARRAY | DISTINCT_VALUES: ARRAY | LENGTH_VALUES:NUMBER | LENGTH_DISTINCT_VALUES: NUMBER | MIN:DATE | MAX:DATE | 
        # ARRAY --  ANY:STRING | COUNT:NUMBER | DISTINCT_COUNT:NUMBER | ARRAY_AGG:ARRAY | DISTINCT_ARRAY_AGG:ARRAY | LENGTH_ARRAY_AGG:NUMBER | LENGTH_DISTINCT_ARRAY_AGG: NUMBER
        'VALUES': [
            {
                "Field": "round",
                "Operation": "COUNT",
                
                # For example, doing COUNT(DISTINCT YEAR(date))
                "DateAggregation": None,
                
                # For example, ARRAY_AGG(DISTINCT value ORDER BY value LIMIT 100)
                "ArrayLimit": None,
            },
        ],
        
        # (4) FILTERS
        # Possible Filters by Field Type:
        # STRING -- EQUALS|DOES_NOT_EQUAL|STARTS_WITH|CONTAINS|DOES_NOT_CONTAIN|IS_IN_ANY|IS_EMPTY|IS_NOT_EMPTY
        # NUMBER -- EQUALS|DOES_NOT_EQUAL|GREATER_THAN|GREATER_THAN_OR_EQUAL_TO|LESS_THAN|LESS_THAN_OR_EQUAL_TO|BETWEEN|IS_IN_ANY
        # DATE   -- EQUALS|DOES_NOT_EQUAL|GREATER_THAN|GREATER_THAN_OR_EQUAL_TO|LESS_THAN|LESS_THAN_OR_EQUAL_TO|BETWEEN|IS_IN_ANY
        # Additionally, if it's an ARRAY type (repeated field), we add in the IS_IN_ALL
        'FILTERS': [
            {"Field": "category", "Operand": "in", "Value": ['web', 'software', 'mobile', 'hardware']},
        ],
        
        # (5) LIMITS
        # Number or None
        'TOTAL_X_LIMIT': 10000,
        'TOTAL_Y_LIMIT': 10000,
                        
        
        # (6) GROUP_BY_X
        # Same as GROUP_BY_X
        'GROUP_BY_Y': [
            {"Field": "category", "Limit": 5, "Sort": "COUNT(round)", "SortDirection": "DESC",},
        ],
        
        
        # (7) SEARCH
        # SearchTypes are [case-insensitive] by FieldType:
        # STRING -- EQUALS|CONTAINS|EDGE|NONE
        # NUMBER -- EQUALS
        # DATE   -- NONE
        
        # *********** QUESTION FOR CLARIFICATION -- IS "EDGE" r'\s\${term}.+?' or just r'^${term}.+?'  ? Much faster if we do "term%" for SQL (but edge is more search/Elasticsearch-ish)
        
        # EXAMPLES:

        # (1) "/my/file/new.csv"
        # CONTAINS (same thing as "Full Text" or LIKE %term% ) would return a match on this. No others would
        
        # (2) "Terminator 2"
        # "2" would be matched on CONTAINS or EDGE(?)
        'SEARCH': {
            'Value': 'hi',
            'Fields': [
                {'Field': 'category', 'Type': 'CONTAINS'},
                {'Field': 'round', 'Type': 'EQUALS'}
            ]
        },

        
        # (8) EXCEPTED_RESULT
        # THIS IS HARD-CODED SO THAT WE CAN MAKE SURE THAT THE QUERY-BUILDER IS GETTING THE CORRECT RESULTS.
        'EXPECTED_RESULT': [('web', 1208), ('software', 102), ('mobile', 48), ('hardware', 39)]
    },
    



]

class TestDataInferrer(unittest.TestCase):

    def test_qury(self):
        
        client = bigquery.Client()
        
        for num, TEST_CASE in enumerate(TEST_CASES):
            agg = Agg()
            query = agg.build_query(TEST_CASE['GROUP_BY'], TEST_CASE['VALUES'], TEST_CASE['TABLE_NAME'])
            print ('RUNNIG QUERY %s of %s:\n\n%s' % (num, len(TEST_CASES), query))
        res = client.query(query)
        results = res.result()
        records = [_.values() for _ in results]
        assert records == TEST_CASE['EXPECTED_RESULT']

            
            
if __name__ == '__main__':
	unittest.main()    

