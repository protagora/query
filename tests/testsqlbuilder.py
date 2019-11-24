import json
import os
import sys
import unittest

sys.path.append(os.path.dirname(os.getcwd()))

from sqlbuilder import (
    AggregationFunction,
    Composer,
    ConfigHandlerGroupBy,
    ConfigHandlerValue,
    ConfigHandlerQuery,
    Configuration,
    GroupByClause,
    GroupByClauses,
    NumberingFunction,
    ValueClause,
    ValueClauses,
    QueryClause,
    StandardSqlFunction,
)

class ConfigurationStub():
    EXPECTED_RESULT = 'EXPECTED_RESULT'
    EXPECTED_QUERY = 'EXPECTED_QUERY'

    def __init__(self, path):
        self._path = None
        self._config = None
        self._setup(path)

    def expected_query(self):
        if self.EXPECTED_QUERY in self.config.keys():
            return self.config[self.EXPECTED_QUERY]
        else:
            return None

    def expected_result(self):
        if self.EXPECTED_RESULT in self.config.keys():
            return self.config[self.EXPECTED_RESULT]
        else:
            return None

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
            raise Exception("Can not open file: {}".format(self.path))
        try:
            self.config = json.loads(data)
        except:
            raise Exception("Not a valid JSON format: {}".format(self.path))

    def __call__(self):
        return self.config

    def __repr__(self):
        return self.path

    def __str__(self):
        return self.path


class TestConfigHandler(unittest.TestCase):

    TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase2.json')

    def setUp(self):
        if not os.path.exists(self.TEST_CONFIG_PATH_BASE):
            raise Exception("Test JSON file not found")
        config = ConfigurationStub(self.TEST_CONFIG_PATH_BASE)
        self.config = config()

    def test_config(self):
        configHandler = ConfigHandlerQuery(self.config)
        configuration = configHandler()

        self.assertEqual(configHandler.config, configuration)


class TestConfigHandlerGroupBy(unittest.TestCase):
    TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase2.json')

    def setUp(self):
        if not os.path.exists(self.TEST_CONFIG_PATH_BASE):
            raise Exception("Test JSON file not found")
        config = ConfigurationStub(self.TEST_CONFIG_PATH_BASE)
        self.config = config()

    def test_config(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        configHandler = ConfigHandlerGroupBy(next(iter(configHandlerQuery().groupby)))
        configuration = configHandler()

        self.assertEqual(configHandler.config, configuration)


class TestQueryObject(unittest.TestCase):

    TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase2.json')

    def setUp(self):
        if not os.path.exists(self.TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(self.TEST_CONFIG_PATH_BASE)
        self.config = config()

    def test_config(self):
        configHandler = ConfigHandlerQuery(self.config)
        configuration = configHandler()
        query = QueryClause(configuration)

        self.assertEqual(query.config, configHandler.config)

    def test_table(self):
        configHandler = ConfigHandlerQuery(self.config)
        configuration = configHandler()
        query = QueryClause(configuration)

        self.assertEqual(query.table, self.config[configHandler.TABLE])

    def test_values(self):
        configHandler = ConfigHandlerQuery(self.config)
        configuration = configHandler()
        query = QueryClause(configuration)

        self.assertEqual(query.values, self.config[configHandler.VALUES])

    def test_limit(self):
        configHandler = ConfigHandlerQuery(self.config)
        configuration = configHandler()
        query = QueryClause(configuration)

        self.assertEqual(query.limit, self.config[configHandler.LIMIT])

    def test_groupby(self):
        configHandler = ConfigHandlerQuery(self.config)
        configuration = configHandler()
        query = QueryClause(configuration)

        self.assertEqual(query.groupby, self.config[configHandler.GROUPBY])


class TestGroupByClause(unittest.TestCase):

    TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase2.json')

    def setUp(self):
        if not os.path.exists(self.TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(self.TEST_CONFIG_PATH_BASE)
        self.config = config()

    def test_config(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        configuration = configHandlerQuery()
        groupByClauses = GroupByClauses(configuration)
        for position, groupByClauseConfig in groupByClauses.clauses.items():
            groupByClause = GroupByClause(groupByClauseConfig)
            self.assertEqual(len(groupByClause.config), len(configuration.groupby[position]))
            self.assertEqual(set(groupByClause.config), set(configuration.groupby[position].values()))

    def test_aggregation(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        configuration = configHandlerQuery()
        groupByClauses = GroupByClauses(configuration)
        for position, groupByClauseConfig in groupByClauses.clauses.items():
            groupByClause = GroupByClause(groupByClauseConfig)
            self.assertEqual(groupByClause.aggregation, configuration.groupby[position][ConfigHandlerGroupBy.DATE_AGGREGATION])

    def test_direction(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        configuration = configHandlerQuery()
        groupByClauses = GroupByClauses(configuration)
        for position, groupByClauseConfig in groupByClauses.clauses.items():
            groupByClause = GroupByClause(groupByClauseConfig)
            self.assertEqual(groupByClause.direction, configuration.groupby[position][ConfigHandlerGroupBy.DIRECTION])

    def test_field(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        configuration = configHandlerQuery()
        groupByClauses = GroupByClauses(configuration)
        for position, groupByClauseConfig in groupByClauses.clauses.items():
            groupByClause = GroupByClause(groupByClauseConfig)
            self.assertEqual(groupByClause.field, configuration.groupby[position][ConfigHandlerGroupBy.FIELD])

    def test_sort(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        configuration = configHandlerQuery()
        groupByClauses = GroupByClauses(configuration)
        for position, groupByClauseConfig in groupByClauses.clauses.items():
            groupByClause = GroupByClause(groupByClauseConfig)
            self.assertEqual(groupByClause.sort, configuration.groupby[position][ConfigHandlerGroupBy.SORT])


class TestGroupByClauses(unittest.TestCase):

    TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase2.json')

    def setUp(self):
        if not os.path.exists(self.TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(self.TEST_CONFIG_PATH_BASE)
        self.config = config()

    def test_clauses(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        groupByClauses = GroupByClauses(configHandlerQuery())

        self.assertEqual(len(configHandlerQuery().values), len(groupByClauses.clauses))

    def test_config(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        groupByClauses = GroupByClauses(configHandlerQuery())

        self.assertEqual(configHandlerQuery.config, groupByClauses.config)


class TestValueClause(unittest.TestCase):

    TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase2.json')

    def setUp(self):
        if not os.path.exists(self.TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(self.TEST_CONFIG_PATH_BASE)
        self.config = config()

    def test_sort(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        configuration = configHandlerQuery()
        valueClauses = ValueClauses(configuration)
        for position, valueClausesConfig in valueClauses.clauses.items():
            valueClause = ValueClause(valueClausesConfig)
            self.assertEqual(valueClause.config, valueClausesConfig)
            self.assertEqual(valueClause.arrayLimit, configuration.values[position][ConfigHandlerValue.ARRAY_LIMIT])


class TestValueClauses(unittest.TestCase):

    TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase2.json')

    def setUp(self):
        if not os.path.exists(self.TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(self.TEST_CONFIG_PATH_BASE)
        self.config = config()

    def test_clauses(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        valueClauses = ValueClauses(configHandlerQuery())

        self.assertEqual(len(configHandlerQuery().values), len(valueClauses.clauses))

    def test_config(self):
        configHandlerQuery = ConfigHandlerQuery(self.config)
        valueClauses = ValueClauses(configHandlerQuery())

        self.assertEqual(configHandlerQuery.config, valueClauses.config)


class TestStandardSqlFunction(unittest.TestCase):

    def setUp(self):
        pass

    def test_instancePattern(self):
        functional = "PATTERN"
        expected_functional = getattr(StandardSqlFunction, "PATTERN", None)
        expression = "expression"
        function = StandardSqlFunction(functional, expression)

        self.assertIsInstance(function, StandardSqlFunction)
        self.assertEqual(function.functional, expected_functional)
        self.assertEqual(function.expression, expression)

    def test_apply(self):
        assert True


class TestNumberingFunction(unittest.TestCase):

    def setUp(self):
        pass

    def test_instanceRank(self):
        functional = "RANK"
        expression = "placeholder"
        function = NumberingFunction(functional, expression)

        self.assertIsInstance(function, NumberingFunction)
        self.assertEqual(function.functional, functional)
        self.assertEqual(function.expression, expression)

    def test_instanceRowNumber(self):
        functional = "ROW_NUMBER"
        expression = "placeholder"
        function = NumberingFunction(functional, expression)

        self.assertIsInstance(function, NumberingFunction)
        self.assertEqual(function.functional, functional)
        self.assertEqual(function.expression, expression)


class TestAggregationFunction(unittest.TestCase):

    def setUp(self):
        pass

    def test_instanceAny(self):
        functional = "ANY"
        expected_functional = "ANY_VALUE"
        expression = "placeholder"
        function = AggregationFunction(functional, expression)

        self.assertIsInstance(function, AggregationFunction)
        self.assertEqual(function.functional, expected_functional)
        self.assertEqual(function.expression, expression)

    def test_instanceArrayAgg(self):
        functional = "ARRAY_AGG"
        expression = "placeholder"
        function = AggregationFunction(functional, expression)

        self.assertIsInstance(function, AggregationFunction)
        self.assertEqual(function.functional, functional)
        self.assertEqual(function.expression, expression)

    def test_instanceAvg(self):
        functional = "AVG"
        expression = "placeholder"
        function = AggregationFunction(functional, expression)

        self.assertIsInstance(function, AggregationFunction)
        self.assertEqual(function.functional, functional)
        self.assertEqual(function.expression, expression)

    def test_instanceCount(self):
        functional = "COUNT"
        expression = "placeholder"
        function = AggregationFunction(functional, expression)

        self.assertIsInstance(function, AggregationFunction)
        self.assertEqual(function.functional, functional)
        self.assertEqual(function.expression, expression)


class TestComposer(unittest.TestCase):

    def setUp(self):
        pass

    def test_instance(self):
        TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase1.json')
        composer = Composer(TEST_CONFIG_PATH_BASE)
        self.assertIsInstance(composer, Composer)

    def test_table(self):
        TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase1.json')
        config = ConfigurationStub(TEST_CONFIG_PATH_BASE)
        composer = Composer(TEST_CONFIG_PATH_BASE)

        self.assertEqual(composer.table, config()[ConfigHandlerQuery.TABLE])

    def test_limit(self):
        TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase1.json')
        config = ConfigurationStub(TEST_CONFIG_PATH_BASE)
        composer = Composer(TEST_CONFIG_PATH_BASE)

        self.assertEqual(composer.table, config()[ConfigHandlerQuery.TABLE])

    def test_groupByClauses(self):
        TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase1.json')

        if not os.path.exists(TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(TEST_CONFIG_PATH_BASE)

        configHandlerQuery = ConfigHandlerQuery(config())
        configInstance = configHandlerQuery()
        groupByClauses = GroupByClauses(configInstance)
        composer = Composer(TEST_CONFIG_PATH_BASE)

        self.assertEqual(composer.groupByClauses.clauses, groupByClauses.clauses)

    def test_valueClauses(self):
        TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase1.json')

        if not os.path.exists(TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(TEST_CONFIG_PATH_BASE)

        configHandlerQuery = ConfigHandlerQuery(config())
        configInstance = configHandlerQuery()
        valueClauses = ValueClauses(configInstance)
        composer = Composer(TEST_CONFIG_PATH_BASE)

        self.assertEqual(composer.valueClauses.clauses, valueClauses.clauses)

    def test_buildQuery(self):
        TEST_CONFIG_PATH_BASE = os.path.join('config', 'testCase1.json')

        if not os.path.exists(TEST_CONFIG_PATH_BASE):
            raise Exception("GroupByField test JSON file not found")
        config = ConfigurationStub(TEST_CONFIG_PATH_BASE)

        composer = Composer(TEST_CONFIG_PATH_BASE)

        expectedQuery = config.expected_query()
        sqlQuery = composer.buildQuery()
        self.assertEqual(sqlQuery, expectedQuery)


if "__main__" == __name__:
    unittest.main()
