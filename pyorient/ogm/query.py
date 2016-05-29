from .operators import (Operator, RelativeOperand, Operand,
                        ArithmeticOperation, LogicalConnective)
from .property import Property, PropertyEncoder
from .element import GraphElement
from .exceptions import MultipleResultsFound, NoResultFound
from .what import What, FunctionWhat, ChainableWhat
from .query_utils import ArgConverter

#from .traverse import Traverse

from collections import namedtuple
from keyword import iskeyword

import sys
if sys.version < '3':
    from itertools import izip_longest as zip_longest
else:
    from itertools import zip_longest

class Query(object):
    def __init__(self, graph, entities):
        """Query against a class or a selection of its properties.

        :param graph: Graph to query
        :param entities: Vertex/Edge class/a collection of its properties,
        an instance of such a class, or a subquery.
        """
        self._graph = graph
        self._subquery = None

        first_entity = entities[0]

        if isinstance(first_entity, Property):
            self.source_name = first_entity._context.registry_name
            self._class_props = entities
        elif isinstance(first_entity, GraphElement):
            # Vertex or edge instance
            self.source_name = first_entity._id
            self._class_props = tuple()
            pass
        elif isinstance(first_entity, Query):# \
                #or isinstance(first_entity, Traverse):
            # Subquery
            self._subquery = first_entity
            self.source_name = first_entity.source_name
            self._class_props = tuple()
            pass
        else:
            self.source_name = first_entity.registry_name
            self._class_props = tuple(entities[1:])

        self._params = {}

    def __iter__(self):
        params = self._params

        # TODO Don't ignore initial skip value
        with TempParams(params, skip='#-1:-1', limit=1):
            optional_clauses = self.build_optional_clauses(params, None)

            prop_names = []
            props = self.build_props(params, prop_names, for_iterator=True)
            if len(prop_names) > 1:
                selectuple = namedtuple(self.source_name + '_props',
                    [name + '_' if iskeyword(name) else name
                        for name in prop_names])
            wheres = self.build_wheres(params)

            g = self._graph
            while True:
                current_skip = params['skip']
                where = u'WHERE {0}'.format(
                    u' and '.join(
                        [self.rid_lower(current_skip)] + wheres))
                select = self.build_select(props, [where] + optional_clauses)

                response = g.client.command(select)
                if response:
                    response = response[0]

                    if prop_names:
                        next_skip = response.oRecordData.get('rid')
                        if next_skip:
                            self.skip(next_skip)

                            if len(prop_names) > 1:
                                yield selectuple(
                                    *tuple(self.parse_record_prop(
                                            response.oRecordData.get(name))
                                        for name in prop_names))
                            else:
                                yield self.parse_record_prop(
                                        response.oRecordData[prop_names[0]])
                        else:
                            yield g.element_from_record(response)
                            break
                    else:
                        if '-' in response._rid:
                            # Further queries would yield the same
                            # TODO Find out if any single iteration queries
                            #      return multiple values
                            yield next(iter(response.oRecordData.values()))
                            break
                        elif response._rid == current_skip:
                            # OrientDB bug?
                            # expand() makes for strange responses
                            break
                        else:
                            self.skip(response._rid)

                        yield g.element_from_record(response)
                else:
                    break

    def __getitem__(self, key):
        """Set query slice, or just get result by index."""
        if isinstance(key, slice):
            if key.stop is None:
                if key.start is not None:
                    self._params['skip'] = key.start
                return self
            elif key.start is None:
                key.start = 0

            return self.slice(key.start, key.stop)

        with TempParams(self._params, skip=key, limit=1):
            response = self.all()
            return response[0] if response else None

    def __str__(self):
        props, where, optional_clauses = self.prepare()
        return self.build_select(props, where + optional_clauses)

    def __len__(self):
        return self.count()

    def prepare(self, prop_names=None):
        params = self._params
        props = self.build_props(params, prop_names)
        skip = params.get('skip')
        if skip and ':' in str(skip):
            rid_clause = [self.rid_lower(skip)]
            skip = None
        else:
            rid_clause = []
        optional_clauses = self.build_optional_clauses(params, skip)

        wheres = rid_clause + self.build_wheres(params)
        where = [u'WHERE {0}'.format(u' and '.join(wheres))] if wheres else []

        return props, where, optional_clauses

    def all(self):
        prop_names = []
        props, where, optional_clauses = self.prepare(prop_names)
        if len(prop_names) > 1:
            selectuple = namedtuple(self.source_name + '_props',
                [name + '_' if iskeyword(name) else name
                    for name in prop_names])
        select = self.build_select(props, where + optional_clauses)

        g = self._graph

        response = g.client.command(select)
        if response:
            # TODO Determine which other queries always take only one iteration
            list_query = 'count' not in self._params

            if list_query:
                if prop_names:
                    if len(prop_names) > 1:
                        return [
                            selectuple(*tuple(
                                self.parse_record_prop(
                                    record.oRecordData.get(name))
                                for name in prop_names))
                            for record in response]
                    else:
                        return [
                            self.parse_record_prop(
                                record.oRecordData[prop_names[0]])
                            for record in response]
                else:
                    if self._params.get('reify', False) and len(response) == 1:
                        # Simplify query for subsequent uses
                        del self._params['kw_filters']
                        self.source_name = response[0]._rid

                    return g.elements_from_records(response)
            else:
                return next(iter(response[0].oRecordData.values()))
        else:
            return []

    def first(self, reify=False):
        with TempParams(self._params, limit=1, reify=reify):
            response = self.all()
            return response[0] if response else None

    def one(self, reify=False):
        with TempParams(self._params, limit=2):
            responses = self.all()
            num_responses = len(responses)
            if num_responses > 1:
                raise MultipleResultsFound(
                    'Expecting one result for query; got more.')
            elif num_responses < 1:
                raise NoResultFound('Expecting one result for query; got none.')
            else:
                return responses[0]

    def scalar(self):
        try:
            response = self.one()
        except NoResultFound:
            return None
        else:
            return response[0] if isinstance(response, tuple) else response

    def count(self, field=None):
        params = self._params

        if not field:
            whats = params.get('what')
            if whats and len(whats) == 1:
                field = self.build_what(whats[0])
            elif len(self._class_props) == 1:
                field = self._class_props[0]
            else:
                field = '*'

        with TempParams(params, count=field):
            return self.all()

    def what(self, *whats):
        self._params['what'] = whats
        return self

    def filter(self, expression):
        self._params['filter'] = expression
        return self

    def filter_by(self, **kwargs):
        self._params['kw_filters'] = kwargs
        return self

    def group_by(self, *criteria):
        self._params['group_by'] = criteria
        return self

    def order_by(self, *criteria, **kwargs):
        self._params['order_by'] = (criteria, kwargs.get('reverse', False))
        return self

    def unwind(self, field):
        self._params['unwind'] = field
        return self

    def skip(self, skip):
        self._params['skip'] = skip
        return self

    def limit(self, limit):
        self._params['limit'] = limit
        return self

    def slice(self, start, stop):
        """Give bounds on how many records to retrieve

        :param start: If a string, must denote the id of the record _preceding_
        that to be retrieved next, or '#-1:-1'. Otherwise denotes how many
        records to skip.

        :param stop: If 'start' was a string, denotes a limit on how many
        records to retrieve. Otherwise, the index one-past-the-last
        record to retrieve.
        """
        self._params['skip'] = start
        if isinstance(start, str):
            self._params['limit'] = stop
        else:
            self._params['limit'] = stop - start
        return self

    def lock(self):
        self._params['lock'] = True

    def filter_string(self, expression_root):
        op = expression_root.operator

        left = expression_root.operands[0]
        right = expression_root.operands[1]
        if isinstance(left, RelativeOperand):
            if isinstance(left, Operand):
                left_str = left.context_name() # Expecting a Property
            elif isinstance(left, ArithmeticOperation):
                left_str = u'({})'.format(self.arithmetic_string(left))
            elif isinstance(left, FunctionWhat):
                left_str = self.build_what(left)
            else:
                raise ValueError(
                    'Operator {} not supported as a filter'.format(op))

            if op is Operator.Equal:
                return u'{0} = {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, self))
            elif op is Operator.GreaterEqual:
                return u'{0} >= {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, self))
            elif op is Operator.Greater:
                return u'{0} > {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, self))
            elif op is Operator.LessEqual:
                return u'{0} <= {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, self))
            elif op is Operator.Less:
                return u'{0} < {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, self))
            elif op is Operator.NotEqual:
                return u'{0} <> {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, self))
            elif op is Operator.Between:
                far_right = PropertyEncoder.encode_value(expression_root.operands[2])
                return u'{0} BETWEEN {1} and {2}'.format(
                    left_str, PropertyEncoder.encode_value(right), far_right)
            elif op is Operator.Contains:
                if isinstance(right, LogicalConnective):
                    return u'{0} contains({1})'.format(
                        left_str, self.filter_string(right))
                else:
                    return u'{} in {}'.format(
                        PropertyEncoder.encode_value(right), left_str)
            elif op is Operator.EndsWith:
                return u'{0} like {1}'.format(left_str, PropertyEncoder.encode_value('%' + right))
            elif op is Operator.Is:
                if not right: # :)
                    return '{0} is null'.format(left_str)
            elif op is Operator.Like:
                return u'{0} like {1}'.format(
                    left_str, PropertyEncoder.encode_value(right))
            elif op is Operator.Matches:
                return u'{0} matches {1}'.format(
                    left_str, PropertyEncoder.encode_value(right))
            elif op is Operator.StartsWith:
                return u'{0} like {1}'.format(
                    left_str, PropertyEncoder.encode_value(right + '%'))
            else:
                raise AssertionError('Unhandled Operator type: {}'.format(op))
        else:
            return u'{0} {1} {2}'.format(
                self.filter_string(left)
                , 'and' if op is Operator.And else 'or'
                , self.filter_string(right))

    def arithmetic_string(self, operation_root):
        if isinstance(operation_root, ArithmeticOperation):
            op = operation_root.operator
            if operation_root.paren:
                lp = '('
                rp = ')'
            else:
                lp = rp = ''

            left = operation_root.operands[0]
            # Unary operators not yet supported?
            right = operation_root.operands[1]

            if op is Operator.Add:
                exp = '{} + {}'.format(
                        self.arithmetic_string(left)
                        , self.arithmetic_string(right))
            elif op is Operator.Sub:
                exp = '{} - {}'.format(
                        self.arithmetic_string(left)
                        , self.arithmetic_string(right))
            elif op is Operator.Mul:
                exp = '{} * {}'.format(
                        self.arithmetic_string(left)
                        , self.arithmetic_string(right))
            elif op is Operator.Div:
                exp = '{} / {}'.format(
                        self.arithmetic_string(left)
                        , self.arithmetic_string(right))
            elif op is Operator.Mod:
                exp = '{} % {}'.format(
                        self.arithmetic_string(left)
                        , self.arithmetic_string(right))

            return '{}{}{}'.format(lp,exp,rp)
        elif isinstance(operation_root, Property):
            return operation_root.context_name()
        else:
            return operation_root


    def build_props(self, params, prop_names=None, for_iterator=False):
        count_field = params.get('count')
        if count_field:
            if isinstance(count_field, Property):
                count_field = count_field.context_name()

            # Record response will use the same (lower-)case as the request
            return ['count({})'.format(count_field or '*')]

        whats = params.get('what')
        if whats:
            props = [self.build_what(what, prop_names) for what in whats]
        else:
            props = [e.context_name() for e in self._class_props]
            if prop_names is not None:
                prop_names.extend(props)

        if props and for_iterator:
            props[0:0] = ['@rid']

        return props

    def build_wheres(self, params):
        kw_filters = params.get('kw_filters')
        kw_where = [u' and '.join(u'{0}={1}'
            .format(PropertyEncoder.encode_name(k), PropertyEncoder.encode_value(v))
                for k,v in kw_filters.items())] if kw_filters else []

        filter_exp = params.get('filter')
        exp_where = [self.filter_string(filter_exp)] if filter_exp else []

        return kw_where + exp_where

    def rid_lower(self, skip):
        return '@rid > {}'.format(skip)

    def build_optional_clauses(self, params, skip):
        optional_clauses = []

        group_by = params.get('group_by')
        if group_by:
            group_clause = 'GROUP BY {}'.format(
                ','.join([by.context_name() for by in group_by]))
            optional_clauses.append(group_clause)

        order_by = params.get('order_by')
        if order_by:
            order_clause = 'ORDER BY {0} {1}'.format(
                ','.join([by.context_name() for by in order_by[0]])
                , 'DESC' if order_by[1] else 'ASC')
            optional_clauses.append(order_clause)

        unwind = params.get('unwind')
        if unwind:
           unwind_clause = 'UNWIND {}'.format(
                    unwind.context_name()
                    if isinstance(unwind, Property) else unwind)
           optional_clauses.append(unwind_clause)

        if skip:
            optional_clauses.append('SKIP {}'.format(skip))

        # TODO Determine other functions for which limit is useless
        if 'count' not in params:
            limit = params.get('limit')
            if limit:
                optional_clauses.append('LIMIT {}'.format(limit))

        lock = params.get('lock')
        if lock:
            optional_clauses.append('LOCK RECORD')

        return optional_clauses

    WhatFunction = namedtuple('what', ['max_args', 'fmt', 'expected'])
    WhatFunctions = {
        # TODO handle GraphElement args
        What.Out: WhatFunction(1, 'out({})', (ArgConverter.Label,))
        , What.In: WhatFunction(1, 'in({})', (ArgConverter.Label,))
        , What.Both: WhatFunction(1, 'both({})', (ArgConverter.Label,))
        , What.OutE: WhatFunction(1, 'outE({})', (ArgConverter.Label,))
        , What.InE: WhatFunction(1, 'inE({})', (ArgConverter.Label,))
        , What.BothE: WhatFunction(1, 'bothE({})', (ArgConverter.Label,))
        , What.OutV: WhatFunction(0, 'outV()', tuple())
        , What.InV: WhatFunction(0, 'inV()', tuple())
        , What.Eval: WhatFunction(1, 'eval({})', (ArgConverter.Expression,))
        , What.Coalesce: WhatFunction(None, 'coalesce({})'
                                      , (ArgConverter.Field,))
        , What.If: WhatFunction(3, 'if({})'
                                , (ArgConverter.Boolean, ArgConverter.Value
                                   , ArgConverter.Value))
        , What.IfNull: WhatFunction(2, 'ifnull({})'
                                    , (ArgConverter.Field, ArgConverter.Value))
        , What.Expand: WhatFunction(1, 'expand({})', (ArgConverter.Field,))
        , What.First: WhatFunction(1, 'first({})', (ArgConverter.Field,))
        , What.Last: WhatFunction(1, 'last({})', (ArgConverter.Field,))
        , What.Count: WhatFunction(1, 'count({})', (ArgConverter.Field,))
        , What.Min: WhatFunction(None, 'min({})', (ArgConverter.Field,))
        , What.Max: WhatFunction(None, 'max({})', (ArgConverter.Field,))
        , What.Avg: WhatFunction(1, 'avg({})', (ArgConverter.Field,))
        , What.Mode: WhatFunction(1, 'mode({})', (ArgConverter.Field,))
        , What.Median: WhatFunction(1, 'median({})', (ArgConverter.Field,))
        , What.Percentile: WhatFunction(None, 'percentile({})'
                                        , (ArgConverter.Field,))
        , What.Variance: WhatFunction(1, 'variance({})', (ArgConverter.Field,))
        , What.StdDev: WhatFunction(1, 'stddev({})', (ArgConverter.Field,))
        , What.Sum: WhatFunction(1, 'sum({})', (ArgConverter.Field,))
        , What.Date: WhatFunction(3, 'date({})'
                                  , (ArgConverter.String, ArgConverter.String
                                     , ArgConverter.String))
        , What.SysDate: WhatFunction(2, 'sysdate({})'
                                     , (ArgConverter.String
                                        , ArgConverter.String))
        , What.Format: WhatFunction(None, 'format({})'
                                    , (ArgConverter.Format, ArgConverter.Field))
        , What.Dijkstra:
            WhatFunction(4, 'dijkstra({})'
                         , (ArgConverter.Vertex, ArgConverter.Vertex
                         , ArgConverter.Label, ArgConverter.Value))
        , What.ShortestPath:
            WhatFunction(4, 'shortestPath({})'
                         , (ArgConverter.Vertex, ArgConverter.Vertex
                            , ArgConverter.Value, ArgConverter.Label))
        , What.Distance:
            WhatFunction(4, 'distance({})'
                         , (ArgConverter.Field, ArgConverter.Field
                            , ArgConverter.Value, ArgConverter.Value))
        , What.Distinct: WhatFunction(1, 'distinct({})', (ArgConverter.Field,))
        , What.UnionAll: WhatFunction(None, 'unionall({})'
                                      , (ArgConverter.Field,))
        , What.Intersect: WhatFunction(None, 'intersect({})'
                                       , (ArgConverter.Field,))
        , What.Difference: WhatFunction(None, 'difference({})'
                                        , (ArgConverter.Field,))
        , What.SymmetricDifference:
            WhatFunction(None, 'symmetricDifference({})', (ArgConverter.Field,))
        # FIXME Don't understand usage of these, yet.
        , What.Set: WhatFunction(1, 'set({})', (ArgConverter.Field,))
        , What.List: WhatFunction(1, 'list({})', (ArgConverter.Field,))
        , What.Map: WhatFunction(2, 'map({})', (ArgConverter.Field
                                                , ArgConverter.Field))
        , What.TraversedElement:
            WhatFunction(2, 'traversedElement({})'
                         , (ArgConverter.Value, ArgConverter.Value))
        , What.TraversedEdge:
            WhatFunction(2, 'traversedEdge({})'
                         , (ArgConverter.Value, ArgConverter.Value))
        , What.TraversedVertex:
            WhatFunction(2, 'traversedVertex({})'
                         , (ArgConverter.Value, ArgConverter.Value))
    }

    def build_what(self, what, prop_names=None):
        if isinstance(what, Property):
            prop_name = what.context_name()
            if prop_names is not None:
                prop_names.append(prop_name)
            return prop_name

        name_override = what.name_override
        as_str = ' AS {}'.format(name_override) if name_override else ''

        if isinstance(what, FunctionWhat):
            what_function = Query.WhatFunctions[what.func]

            if prop_names is not None:
                # Projections not allowed with Expand
                counted = what.func is not What.Expand
                if counted:
                    prop_names.append(
                        Query.parse_prop_name(what_function.fmt, name_override))

            return '{}{}'.format(
                what_function.fmt.format(
                    ','.join(self.what_args(what_function.expected,
                                            what.args))), as_str)
        elif isinstance(what, ChainableWhat):
            chain = []
            for func_args in what.chain:
                what_function = Query.WhatFunctions[func_args[0]]
                max_args = what_function.max_args
                if max_args > 0 or max_args is None:
                    chain.append(
                        what_function.fmt.format(
                            ','.join(self.what_args(what_function.expected,
                                                    func_args[1]))))
                else:
                    chain.append(what_function.fmt)
            chain.extend(what.props)

            if prop_names is not None:
                prop_names.append(
                    Query.parse_prop_name(chain[0], name_override))
            return '{}{}'.format('.'.join(chain), as_str)

    @staticmethod
    def parse_prop_name(from_str, override):
        return override if override else from_str[:from_str.index('(')]

    def what_args(self, expected, args):
        if args:
            return [ArgConverter.convert_to(conversion, arg, self)
                    for arg, conversion in
                        zip_longest(args, expected
                                    , fillvalue=expected[-1])
                        if arg is not None]
        else:
            return []

    def build_select(self, props, optional_clauses):
        # This 'is not None' is important; don't want to implicitly call
        # __len__ (which invokes count()) on subquery.
        if self._subquery is not None:
            src = u'({})'.format(self._subquery)
        else:
            src = self.source_name

        optional_string = ' '.join(optional_clauses)
        if props:
            return u'SELECT {} FROM {} {}'.format(
                ','.join(props), src, optional_string)
        else:
            return u'SELECT FROM {} {}'.format(src, optional_string)

    def parse_record_prop(self, prop):
        if isinstance(prop, list):
            g = self._graph
            if len(prop) > 1:
                return g.elements_from_links(prop)
            elif len(prop) == 1:
                return g.element_from_link(prop[0])
            else:
                return None
        return prop

class TempParams(object):
    def __init__(self, params, **kwargs):
        self.params = params
        self.overrides = kwargs
        self.old = {}

    def __enter__(self):
        # Save overridden, overwrite
        for k,v in self.overrides.items():
            self.old[k] = self.params.get(k)
            self.params[k] = v

    def __exit__(self, type, value, traceback):
        for k,v in self.old.items():
            if v is None:
                del self.params[k]
            else:
                self.params[k] = v

