from .operators import Operator, Conditional
from .property import Property
from .exceptions import MultipleResultsFound, NoResultFound

from collections import namedtuple

class Query(object):
    def __init__(self, graph, entities):
        """Query against a class or a selection of its properties.

        :param graph: Graph to query
        :param entities: Vertex/Edge class, _or_ a collection of its properties
        """
        self._graph = graph

        first_entity = entities[0]

        if isinstance(first_entity, Property):
            self._element_class = first_entity._context
            self._class_props = entities
        else:
            self._element_class = first_entity
            self._class_props = tuple()

        self._params = {}

    def __iter__(self):
        params = self._params

        with TempParams(params, skip='#-1:-1', limit=1):
            optional_clauses = self.build_optional_clauses(params, None)

            # TODO Determine which other queries always take only one iteration
            class_propnames = tuple(
                prop.context_name() for prop in self._class_props)
            selecting_props = len(class_propnames) > 0
            #multiple_iterations = 'count' not in params
            multiple_iterations = True
            include_rid = selecting_props and multiple_iterations

            props = self.build_props(params, include_rid)
            wheres = self.build_wheres(params)

            g = self._graph
            while True:
                where = 'WHERE {0}'.format(
                    ' and '.join(
                        [self.rid_lower(params['skip'])]
                        if multiple_iterations else [] + wheres))
                select = self.build_select(props, [where] + optional_clauses)
                response = g.client.command(select)
                if response:
                    response = response[0]
                    if include_rid:
                        self.skip(response.oRecordData['rid'])
                    elif '-' in response._rid:
                        # Further queries would yield the same
                        # TODO Find out if any single iteration queries return
                        #      multiple values
                        yield next(iter(response.oRecordData.values()))
                        break
                    else:
                        self.skip(response._rid)

                    if selecting_props:
                        if len(class_propnames) > 1:
                            selectuple = namedtuple(
                                self._element_class.registry_name + '_props',
                                class_propnames)
                            yield selectuple(
                                *tuple(response.oRecordData.get(name)
                                    for name in class_propnames))
                        else:
                            yield response.oRecordData[class_propnames[0]]
                    else:
                        yield g.element_from_record(response)
                else:
                    break

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.slice(key.start, key.stop)
        return self.slice(key, key+1)

    def __len__(self):
        return self.count()

    def all(self):
        params = self._params
        props = self.build_props(params)
        skip = params.get('skip')
        if skip and ':' in str(skip):
            rid_clause = [self.rid_lower(skip)]
            skip = None
        else:
            rid_clause = []
        optional_clauses = self.build_optional_clauses(params, skip)

        wheres = rid_clause + self.build_wheres(params)
        where = ['WHERE {0}'.format(' and '.join(wheres))] if wheres else []
        select = self.build_select(props, where + optional_clauses)

        g = self._graph
        response = g.client.command(select)
        if response:
            # TODO Determine which other queries always take only one iteration
            list_query = 'count' not in params

            if list_query:
                class_propnames = tuple(
                    prop.context_name() for prop in self._class_props)
                selecting_props = len(class_propnames) > 0
                if selecting_props:
                    if len(class_propnames) > 1:
                        # TODO Convert to namedtuple
                        selectuple = namedtuple(
                            self._element_class.registry_name + '_props',
                            class_propnames)

                        return [
                            selectuple(*tuple(record.oRecordData.get(name)
                                       for name in class_propnames))
                            for record in response]
                    else:
                        return [record.oRecordData[class_propnames[0]]
                                for record in response]
                else:
                    return g.elements_from_records(response)
            else:
                return next(iter(response[0].oRecordData.values()))
        else:
            return []

    def first(self):
        with TempParams(self._params, limit=1):
            response = self.all()
            return response[0] if response else None

    def one(self):
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
        with TempParams(self._params, count=field):
            return self.all()

    def filter(self, expression):
        self._params['filter'] = expression
        return self

    def filter_by(self, **kwargs):
        self._params['kw_filters'] = kwargs
        return self

    def group_by(self, *criteria):
        self._params['group_by'] = criteria
        return self

    def order_by(self, *criteria):
        self._params['order_by'] = criteria
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

    def filter_string(self, expression_root):
        op = expression_root.operator

        left = expression_root.operands[0]
        right = expression_root.operands[1]
        if isinstance(left, Conditional):
            left_name = left.context_name()

            if op is Operator.Equal:
                return '{0}={1}'.format(
                    left_name, repr(right) if isinstance(right, str) else right)
            elif op is Operator.GreaterEqual:
                return '{0}>={1}'.format(
                    left_name, repr(right) if isinstance(right, str) else right)
            elif op is Operator.Greater:
                return '{0}>{1}'.format(
                    left_name, repr(right) if isinstance(right, str) else right)
            elif op is Operator.LessEqual:
                return '{0}<={1}'.format(
                    left_name, repr(right) if isinstance(right, str) else right)
            elif op is Operator.Less:
                return '{0}<{1}'.format(
                    left_name, repr(right) if isinstance(right, str) else right)
            elif op is Operator.NotEqual:
                return '{0}<>{1}'.format(
                    left_name, repr(right) if isinstance(right, str) else right)
            elif op is Operator.Between:
                far_right = expression_root.operands[2]
                if isinstance(far_right, str):
                    far_right = repr(far_right)
                return '{0} BETWEEN {1} and {2}'.format(
                    left_name, right, far_right)
            elif op is Operator.Contains:
                return '{0} contains({1})'.format(
                    left_name, self.filter_string(right))
            elif op is Operator.EndsWith:
                return '{0} like \'%{1}\''.format(left_name, right)
            elif op is Operator.Is:
                if not right: # :)
                    return '{0} is null'.format(left_name)
            elif op is Operator.Like:
                return '{0} like \'{1}\''.format(
                    left_name, right)
            elif op is Operator.Matches:
                return '{0} matches \'{1}\''.format(
                    left_name, right)
            elif op is Operator.StartsWith:
                return '{0} like \'{1}%\''.format(
                    left_name, right)
            else:
                raise ValueError(
                    'Operator {} not supported as a filter'.format(op))
        else:
            return '{0} {1} {2}'.format(
                self.filter_string(left)
                , 'and' if op is Operator.And else 'or'
                , self.filter_string(right))

    def build_props(self, params, include_rid=False):
        props = None
        if 'count' in params:
            field = params['count']
            if isinstance(field, Property):
                field = field.context_name()

            # Record response will use the same (lower-)case as the request
            props = ['count({})'.format(field or '*')]
        else:
            props = [e.context_name() for e in self._class_props]
            if include_rid:
                props[0:0] = ['@rid']

        return props

    def build_wheres(self, params):
        kw_filters = params.get('kw_filters')
        kw_where = [' and '.join('{0}={1}'
            .format(k, repr(v) if isinstance(v, str) else v)
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
            # FIXME Support ascending/descending specification
            order_clause = 'ORDER BY {}'.format(
                ','.join([by.context_name() for by in order_by]))

        if skip:
            optional_clauses.append('SKIP {}'.format(skip))

        # TODO Determine other functions for which limit is useless
        if 'count' not in params:
            limit = params.get('limit')
            if limit:
                optional_clauses.append('LIMIT {}'.format(limit))

        return optional_clauses

    def build_select(self, props, optional_clauses):
        return 'SELECT {0} FROM {1} {2}'.format(
                    ','.join(props)
                    , self._element_class.registry_name
                    , ' '.join(optional_clauses))

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

