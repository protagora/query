from collections import namedtuple
from google.cloud import bigquery
import json
import logging
import os

logger = logging.getLogger('SQLBuilder')

class Field(object):
    REPRESENTATION = "{}-{}-{}"
    STRING = "{} ({})"
    def __init__(self, name, index, alias, modifier, order, limit):
        self._alias = None
        self._index = None
        self._name = None
        self._modifier = None
        self._order = None
        self._limit = None
        self._setup(name, index, alias, modifier, order, limit)

    def _setup(self, name, index, alias, modifier, order, limit):
        self.alias = alias
        self.index = index
        self.limit = limit
        self.modifier = modifier
        self.name = name
        self.order = order

    @property
    def alias(self):
        return self._alias
    @alias.setter
    def alias(self, alias):
        alias = str(alias)
        self._alias = alias

    @property
    def index(self):
        return self._index
    @index.setter
    def index(self, index):
        self._index = index

    @property
    def limit(self):
        return self._limit
    @limit.setter
    def limit(self, limit):
        self._limit = limit

    @property
    def modifier(self):
        return self._modifier
    @modifier.setter
    def modifier(self, modifier):
        self._modifier = modifier

    @property
    def name(self):
        return self._name
    @name.setter
    def name(self, name):
        self._name = name

    @property
    def order(self):
        return self._order
    @order.setter
    def order(self, order):
        self._order = order

    def __str__(self):
        return str(self.alias)

    def __repr__(self):
        return self.REPRESENTATION.format(str(self.index), str(self.name), str(self.alias))

class FieldCollection(object):
    OPERATION_ALIAS = '{}({}) AS {}'
    OPERATION = '{}({})'
    FIELD_ALIAS = '{} AS {}'
    HANDLE = '{}_{}'

    def __init__(self):
        self._fieldsCount = -1
        self._fieldsByIndex = {}

    def __len__(self):
        return self._fieldsCount + 1

    def addField(self, fieldName, alias=None, operation=None, modifier=None, order=None, limit=None):
        self._fieldsCount += 1
        if operation is None:
            if alias is None:
                handle = fieldName
            else:
                handle = self.FIELD_ALIAS.format(fieldName, alias)
        else:
            if alias is None:
                handle = self.OPERATION.format(operation, fieldName)
            else:
                handle = self.OPERATION_ALIAS.format(operation, fieldName, alias)
        field = Field(fieldName, self._fieldsCount, handle, modifier, order, limit)
        self._fieldsByIndex[self._fieldsCount] = field
        return self._fieldsCount

    def getByIndex(self, index):
        if index in self._fieldsByIndex.keys():
            return self._fieldsByIndex(index)
        else:
            return None

    def getAll(self):
        return list(self._fieldsByIndex.values())

class StandardSqlFunction(object):
    PATTERN = "{}({})"
    NOT_SUPPORTED = "Standard function '{}' not supported"

    def __init__(self, functional, expression):
        self._functional = None
        self._expression = None
        self._setup(functional, expression)

    def _setup(self, functional, expression):
        self.functional = str(functional)
        self.expression = str(expression)

    @property
    def functional(self):
        return self._functional
    @functional.setter
    def functional(self, functional):
        functional = getattr(self, functional, None)
        if functional is None:
            logger.error(self.NOT_SUPPORTED.format(functional))
            raise Exception(self.NOT_SUPPORTED.format(functional))
        self._functional = functional

    @property
    def expression(self):
        return self._expression
    @expression.setter
    def expression(self, expression):
        self._expression = expression
    @expression.deleter
    def expression(self):
        del self._expression

    def apply(self):
        return self.PATTERN.format(self.functional, self.expression)


class NumberingFunction(StandardSqlFunction):
    DENSE_RANK      = "DENSE_RANK"
    RANK            = "RANK"
    ROW_NUMBER      = "ROW_NUMBER"

    NOT_SUPPORTED   = "Numbering function '{}' not supported"


class AggregationFunction(StandardSqlFunction):
    ANY         = "ANY_VALUE"
    ARRAY_AGG   = "ARRAY_AGG"
    AVG         = "AVG"
    COUNT       = "COUNT"
    MIN         = "MIN"
    MAX         = "MAX"
    SUM         = "SUM"

    NOT_SUPPORTED = "Aggregation function '{}' not supported"


class QueryClause(object):

    def __init__(self, config):
        self._table = None
        self._limit = None
        self._values = None
        self._groupby = None
        self._config = None
        self._setup(config)

    @property
    def table(self):
        return self._table
    @table.setter
    def table(self, table):
        self._table = table

    @property
    def limit(self):
        return self._limit
    @limit.setter
    def limit(self, limit):
        self._limit = limit

    @property
    def values(self):
        return self._values
    @values.setter
    def values(self, values):
        self._values = values

    @property
    def groupby(self):
        return self._groupby
    @groupby.setter
    def groupby(self, groupby):
        self._groupby = groupby

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self, config):
        self._config = config

    def _setup(self, config):
        self.config = config
        self._parse()

    def _parse(self):
        self.table = self.config.table
        self.limit = self.config.limit
        self.values = self.config.values
        self.groupby = self.config.groupby


class GroupByClauses(object):
    NOT_VALID_STRUCTURE = "GroupBy '{}' not a valid structure"

    def __init__(self, config):
        self._clauses = None
        self._config = None
        self._setup(config)

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self, config):
        self._config = config
        self._parse()

    @property
    def clauses(self):
        return self._clauses
    @clauses.setter
    def clauses(self, clauses):
        self._clauses = clauses

    def _parse(self):
        if self.config.groupby is None:
            self.clauses = None
        else:
            for position, groupby in enumerate(self.config.groupby):
                try:
                    handler = ConfigHandlerGroupBy(groupby)
                    self.clauses[position] = handler()
                except:
                    logger.info(self.NOT_VALID_STRUCTURE.format(position))

    def _setup(self, config):
        self.clauses = {}
        self.config = config


class GroupByClause(object):
    def __init__(self, config):
        self._aggregation = None
        self._config = None
        self._direction = None
        self._field = None
        self._limit = None
        self._sort = None
        self._setup(config)

    @property
    def aggregation(self):
        return self._aggregation
    @aggregation.setter
    def aggregation(self, aggregation):
        self._aggregation = aggregation

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self, config):
        self._config = config
        self._parse()

    @property
    def direction(self):
        return self._direction
    @direction.setter
    def direction(self, direction):
        self._direction = direction

    @property
    def field(self):
        return self._field
    @field.setter
    def field(self, field):
        self._field = field

    @property
    def sort(self):
        return self._sort
    @sort.setter
    def sort(self, sort):
        self._sort = sort

    def _parse(self):
        self.aggregation = self.config.aggregation
        self.direction = self.config.direction
        self.field = self.config.field
        self.limit = self.config.limit
        self.sort = self.config.sort

    def _setup(self, config):
        self.config = config


class ValueClauses(object):
    NOT_VALID_STRUCTURE = "Value '{}' not a valid structure"

    def __init__(self, config):
        self._clauses = None
        self._config = None
        self._setup(config)

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self, config):
        self._config = config
        self._parse()

    @property
    def clauses(self):
        return self._clauses
    @clauses.setter
    def clauses(self, clauses):
        self._clauses = clauses

    def _parse(self):
        if self.config.values is None:
            self.clauses = None
        else:
            for position, value in enumerate(self.config.values):
                try:
                    handler = ConfigHandlerValue(value)
                    self.clauses[position] = handler()
                except:
                    logger.info(self.NOT_VALID_STRUCTURE.format(position))

    def _setup(self, config):
        self.clauses = {}
        self.config = config


class ValueClause(object):
    def __init__(self, config):
        self._arrayLimit = None
        self._config = None
        self._dateAggregation = None
        self._field = None
        self._operation = None
        self._setup(config)

    @property
    def dateAggregation(self):
        return self._dateAggregation
    @dateAggregation.setter
    def dateAggregation(self, dateAggregation):
        self._dateAggregation = dateAggregation

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self, config):
        self._config = config

    @property
    def field(self):
        return self._field
    @field.setter
    def field(self, field):
        self._field = field

    @property
    def arrayLimit(self):
        return self._arrayLimit
    @arrayLimit.setter
    def arrayLimit(self, arrayLimit):
        self._arrayLimit = arrayLimit

    @property
    def operation(self):
        return self._operation
    @operation.setter
    def operation(self, operation):
        self._operation = operation

    def _parse(self):
        self.dateAggregation = self.config.dateAggregation
        self.field = self.config.field
        self.arrayLimit = self.config.arrayLimit
        self.operation = self.config.operation

    def _setup(self, config):
        self.config = config


class Composer(object):

    EXPRESSION = "{} {}"
    STATEMENT_FROM = "FROM"
    STATEMENT_GROUPBY = "GROUP BY"
    STATEMENT_LIMIT = "LIMIT"
    STATEMENT_SELECT = "SELECT"
    STATEMENT_ORDERBY = "ORDER BY"
    SEPARATOR_CLAUSES = " "
    SEPARATOR_FIELDS = ", "
    TABLE_NAME_ESC = "`{}`"
    QUALIFIER_DISTINCT = "DISTINCT"

    def __init__(self, path):
        self._aliases = None
        self._fields = None
        self._groupByClauses = None
        self._groupByFields = None
        self._path = None
        self._sql = None
        self._table = None
        self._query = None
        self._setup(path)

    def buildQuery(self):
        self.sql = []
        self._with()
        self._select()
        self._from()
        self._where()
        self._groupby()
        self._limit()
        self._offset()
        return self.SEPARATOR_CLAUSES.join(
            [
                partial for partial in self.sql if len(partial)
            ]
        )

    def _with(self):
        pass

    def _select(self):
        self.sql.append(
            self.EXPRESSION.format(
                self.STATEMENT_SELECT,
                self.SEPARATOR_FIELDS.join(
                    [str(field) for field in self.fields.getAll()]
                )
            )
        )

    def _from(self):
        self.sql.append(
            self.EXPRESSION.format(
                self.STATEMENT_FROM,
                self.TABLE_NAME_ESC.format(self.table)
            )
        )

    def _where(self):
        pass

    def _groupby(self):
        if self.groupByFields:
            self.sql.append(self.EXPRESSION.format(
                self.STATEMENT_GROUPBY,
                self.SEPARATOR_FIELDS.join(
                    [str(field) for field in self.groupByFields.getAll()]
                )
            )
        )

    def _limit(self):
        if self.limit:
            self.sql.append(
                self.EXPRESSION.format(
                    self.STATEMENT_LIMIT,
                    self.limit
                )
            )

    def _offset(self):
        if self.offset:
            self.sql.append(
                self.EXPRESSION.format(
                    self.STATEMENT_OFFSET,
                    self.offset
                )
            )

    def _setup(self, path):
        self.path = path
        self.config = Configuration(self.path)
        self._parseConfig()

    @property
    def groupByClauses(self):
        return self._groupByClauses
    @groupByClauses.setter
    def groupByClauses(self, groupByClauses):
        self._groupByClauses = groupByClauses

    @property
    def fields(self):
        return self._fields
    @fields.setter
    def fields(self, fields):
        self._fields = fields

    @property
    def groupByFields(self):
        return self._groupByFields
    @groupByFields.setter
    def groupByFields(self, groupByFields):
        self._groupByFields = groupByFields

    @property
    def path(self):
        return self._path
    @path.setter
    def path(self, path):
        self._path = path

    @property
    def sql(self):
        return self._sql
    @sql.setter
    def sql(self, sql):
        self._sql = sql

    @property
    def table(self):
        return self._table
    @table.setter
    def table(self, table):
        self._table = table

    @property
    def valueClauses(self):
        return self._valueClauses
    @valueClauses.setter
    def valueClauses(self, valueClauses):
        self._valueClauses = valueClauses

    def _groupbyClauses(self, groupByClauses):
        self.groupByClauses = groupByClauses

    def _values(self, valueClauses):
        self.valueClauses = valueClauses

    def addTable(self, tableName):
        self.table = tableName

    def _parseConfig(self):
        self._parseTable()
        self._parseGroupBy()
        self._parseValues()
        self._parseLimit()
        self._parseOffset()
        self._collectFields()

    def _parseTable(self):
        self.table = self.config.config[ConfigHandlerQuery.TABLE]

    def _parseGroupBy(self):
        configHandlerQuery = ConfigHandlerQuery(self.config.config)
        configInstance = configHandlerQuery()
        self.groupByClauses = GroupByClauses(configInstance)

    def _parseValues(self):
        configHandlerQuery = ConfigHandlerQuery(self.config.config)
        configuration = configHandlerQuery()
        self.valueClauses = ValueClauses(configuration)

    def _parseLimit(self):
        if ConfigHandlerQuery.LIMIT in self.config.config.keys():
            self.limit = self.config.config[ConfigHandlerQuery.LIMIT]
        else:
            self.limit = None

    def _parseOffset(self):
        if ConfigHandlerQuery.OFFSET in self.config.config.keys():
            self.offset = self.config.config[ConfigHandlerQuery.OFFSET]
        else:
            self.offset = None

    def _collectFields(self):
        self.fields = FieldCollection()
        self.groupByFields = FieldCollection()
        for index, groupByClause in self.groupByClauses.clauses.items():
            if groupByClause.alias:
                self.fields.addField(
                    groupByClause.field,
                    alias=groupByClause.alias
                )
            else:
                self.fields.addField(groupByClause.field)
            self.groupByFields.addField(groupByClause.field)
        for index, valueClause in self.valueClauses.clauses.items():
            # @TODO: Add support for date aggregation
            expression = valueClause.field
            if valueClause.modifier is not None:
                expression = self.EXPRESSION.format(valueClause.modifier, expression)
            if valueClause.order is not None:
                order = self.EXPRESSION.format(self.STATEMENT_ORDERBY, valueClause.field)
                direction = self.EXPRESSION.format(order, valueClause.direction)
                expression = self.EXPRESSION.format(expression, direction)
            if valueClause.operation == AggregationFunction.ARRAY_AGG and valueClause.arrayLimit:
                limit = self.EXPRESSION.format(self.STATEMENT_LIMIT, valueClause.arrayLimit)
                expression = self.EXPRESSION.format(expression, limit)
            expression = AggregationFunction(
                valueClause.operation,
                expression,
            ).apply()
            self.fields.addField(expression, alias=valueClause.alias)

class Configuration():

    def __init__(self, path):
        self._path = None
        self._config = None
        self._setup(path)

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


class ConfigurationHandler(object):
    MISSING_FIELD = "Missing mandatory configuration field: {}"

    def __init__(self, config):
        self._config = config
        self._setup(config)

    def _setup(self, config):
        self.config = config
        self._parse()

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self, config):
        self._config = config

    def __call__(self):
        return self.config


class ConfigHandlerQuery(ConfigurationHandler):
    GROUPBY = 'GROUP_BY'
    LIMIT   = 'TOTAL_LIMIT'
    OFFSET  = 'OFFSET'
    TABLE   = 'TABLE_NAME'
    VALUES  = 'VALUES'

    def _parse(self):
        try:
            table = self.config[self.TABLE]
            limit = None
            if self.LIMIT in self.config.keys():
                limit = self.config[self.LIMIT]
            values = None
            if self.VALUES in self.config.keys():
                values = self.config[self.VALUES]
            groupby = None
            if self.GROUPBY in self.config.keys():
                groupby = self.config[self.GROUPBY]
        except:
            logger.error(self.MISSING_TABLE.format(self.TABLE))
            raise Exception(self.MISSING_TABLE.format(self.TABLE))
        Configuration = namedtuple(
            'Configuration',
            'table limit values groupby'
        )
        self.config = Configuration(table, limit, values, groupby)


class ConfigHandlerValue(ConfigurationHandler):
    ARRAY_LIMIT = 'ArrayLimit'
    DATE_AGGREGATION = 'DateAggregation'
    FIELD = 'Field'
    OPERATION = 'Operation'
    MODIFIER = 'Modifier'
    ORDER = 'Order'
    DIRECTION = 'Direction'
    ALIAS = 'Alias'

    def _parse(self):
        try:
            _field = self.config[self.FIELD]
            _arrayLimit = None
            if self.ARRAY_LIMIT in self.config.keys():
                _arrayLimit = self.config[self.ARRAY_LIMIT]
            _alias = None
            if self.ALIAS in self.config.keys():
                _alias = self.config[self.ALIAS]
            _dateAggregation = None
            if self.DATE_AGGREGATION in self.config.keys():
                _dateAggregation = self.config[self.DATE_AGGREGATION]
            _direction = None
            if self.DIRECTION in self.config.keys():
                _direction = self.config[self.DIRECTION]
            _operation = None
            if self.OPERATION in self.config.keys():
                _operation = self.config[self.OPERATION]
            _modifier = None
            if self.MODIFIER in self.config.keys():
                _modifier = self.config[self.MODIFIER]
            _order = None
            if self.ORDER in self.config.keys():
                _order = self.config[self.ORDER]
        except:
            logger.error(self.MISSING_FIELD.format(self.FIELD))
            raise Exception(self.MISSING_FIELD.format(self.FIELD))
        Configuration = namedtuple(
            'Configuration',
            'arrayLimit alias dateAggregation field modifier operation order direction'
        )
        self.config = Configuration(
            _arrayLimit,
            _alias,
            _dateAggregation,
            _field,
            _modifier,
            _operation,
            _order,
            _direction
        )


class ConfigHandlerGroupBy(ConfigurationHandler):
    DATE_AGGREGATION    = 'DateAggregation'
    DIRECTION           = 'SortDirection'
    FIELD               = 'Field'
    LIMIT               = 'Limit'
    SORT                = 'Sort'
    ALIAS               = 'Alias'

    def _parse(self):
        try:
            field = self.config[self.FIELD]
            aggregation = None
            if self.DATE_AGGREGATION in self.config.keys():
                aggregation = self.config[self.DATE_AGGREGATION]
            alias = None
            if self.ALIAS in self.config.keys():
                alias = self.config[self.ALIAS]
            direction = None
            if self.DIRECTION in self.config.keys():
                direction = self.config[self.DIRECTION]
            limit = None
            if self.LIMIT in self.config.keys():
                limit = self.config[self.LIMIT]
            sort = None
            if self.SORT in self.config.keys():
                sort = self.config[self.SORT]
        except:
            logger.error(self.MISSING_FIELD.format(self.FIELD))
            raise Exception(self.MISSING_FIELD.format(self.FIELD))
        Configuration = namedtuple(
            'Configuration',
            'aggregation alias direction field limit sort'
        )
        self.config = Configuration(
            aggregation,
            alias,
            direction,
            field,
            limit,
            sort
        )


if "__main__" == __name__:
    print("SQLBuilder is a package file, execution has no effects.\nTo execute tests suite run testsqlbuilder.py")
