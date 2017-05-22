from .property import Property, PropertyEncoder
from .element import GraphElement
from .exceptions import MultipleResultsFound, NoResultFound
from .query_utils import ArgConverter
from .expressions import ExpressionMixin
from .commands import Command

#from .traverse import Traverse

from pyorient import OrientRecordLink

from collections import namedtuple
from keyword import iskeyword

import sys
if sys.version < '3':
    import string
    sanitise_ids = string.maketrans('#:', '__')
else:
    sanitise_ids = {
        ord('#'): '_'
        , ord(':'): '_'
    }

class Query(ExpressionMixin, Command):
    def __init__(self, graph, entities):
        """Query against a class or a selection of its properties.

        :param graph: Graph to query
        :param entities: Vertex/Edge class/a collection of its properties,
        an instance of such a class, or a subquery.
        """
        self._graph = graph
        self._subquery = None
        self._params = {}

        first_entity = entities[0]

        from .what import What, LetVariable

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
        elif isinstance(first_entity, LetVariable):
            self.source_name = self.build_what(first_entity)
            self._class_props = tuple()
        elif isinstance(first_entity, What):
            self._params['what'] = [first_entity]
            self.source_name = None
            self._class_props = tuple()
        else:
            self.source_name = first_entity.registry_name
            self._class_props = tuple(entities[1:])

    @classmethod
    def sub(cls, source):
        """Shorthand for defining a sub-query, which does not need a Graph"""
        return cls(None, (source, ))

    def query(self):
        """Create a query, with current query as a subquery.
        Serves as a useful shorthand for chaining sub-queries."""
        return Query(self._graph, (self, ))

    @property
    def graph(self):
        """Get graph being queried. May be None for subqueries"""
        return self._graph

    def __iter__(self):
        params = self._params

        # TODO Don't ignore initial skip value
        with TempParams(params, skip='#-1:-1', limit=1):
            optional_clauses = self.build_optional_clauses(params, None)

            prop_names = []
            props, lets = self.build_props(params, prop_names, for_iterator=True)
            if len(prop_names) > 1:
                prop_prefix = self.source_name.translate(sanitise_ids)

                selectuple = namedtuple(prop_prefix + '_props',
                    [Query.sanitise_prop_name(name)
                        for name in prop_names])
            wheres = self.build_wheres(params)

            g = self._graph
            while True:
                current_skip = params['skip']
                where = u'WHERE {0}'.format(
                    u' and '.join(
                        [self.rid_lower(current_skip)] + wheres))

                select = self.build_select(props, lets + [where] + optional_clauses)

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
        props, lets, where, optional_clauses = self.prepare()
        return self.build_select(props, lets + where + optional_clauses)

    def __len__(self):
        return self.count()

    def prepare(self, prop_names=None):
        params = self._params
        props, lets = self.build_props(params, prop_names)
        skip = params.get('skip')
        if skip and ':' in str(skip):
            rid_clause = [self.rid_lower(skip)]
            skip = None
        else:
            rid_clause = []
        optional_clauses = self.build_optional_clauses(params, skip)

        wheres = rid_clause + self.build_wheres(params)
        where = [u'WHERE {0}'.format(u' and '.join(wheres))] if wheres else []

        return props, lets, where, optional_clauses

    def all(self):
        prop_names = []
        props, lets, where, optional_clauses = self.prepare(prop_names)
        if len(prop_names) > 1:
            prop_prefix = self.source_name.translate(sanitise_ids)

            selectuple = namedtuple(prop_prefix + '_props',
                [Query.sanitise_prop_name(name)
                    for name in prop_names])
        select = self.build_select(props, lets + where + optional_clauses)

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
                        prop_name = prop_names[0]
                        return [
                            self.parse_record_prop(
                                record.oRecordData[prop_name])
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
        """Get the first query match.
        If expecting a single edge or vertex and this returns a list,
        you likely intended to expand() your what() argument.
        """
        with TempParams(self._params, limit=1, reify=reify):
            response = self.all()
            return response[0] if response else None

    def one(self, reify=False):
        """Raises exception when there's anything but a single match, otherwise
        returns match.
        If expecting a single edge or vertex and this returns a list,
        you likely intended to expand() your what() argument.
        """
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

    def let(self, *ordered, **kwargs):
        """Define LET block for query, setting context variables.
        :param ordered: When context variables have dependencies, order
        matters. In Python < 3.6, OrderedDict will not retain order as kwargs,
        pass them here. See PEP 468.
        :param kwargs: Conveniently specify context variables.
        """
        if ordered:
            if kwargs is not None:
                ordered[0].update(kwargs)
            self._params['let'] = ordered[0]
        else:
            self._params['let'] = kwargs
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

    def order_by(self, *criteria):
        """:param criteria: A projection field, or a 2-tuple of the form
        (<projection field>, <reverse>), where <reverse> is a bool which
        - if True - results in a descending order for the field"""
        self._params['order_by'] = criteria
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

    def build_props(self, params, prop_names=None, for_iterator=False):
        let = params.get('let')
        if let:
            lets = ['LET {}'.format(
                ','.join('{} = {}'.format(
                    PropertyEncoder.encode_name(k),
                    u'({})'.format(v) if isinstance(v, Query) else
                    self.build_what(v)) for k,v in let.items()))]
        else:
            lets = []

        count_field = params.get('count')
        if count_field:
            if isinstance(count_field, Property):
                count_field = count_field.context_name()

            # Record response will use the same (lower-)case as the request
            return ['count({})'.format(count_field or '*')], lets

        whats = params.get('what')
        if whats:
            props = [self.build_what(what, prop_names) for what in whats]

            if prop_names is not None:
                # Multiple, distinct what's can alias to the same name
                # Make unique; consistent with what OrientDB assumes
                used_names = {}
                for idx, name in enumerate(prop_names):
                    prop_names[idx] = Query.unique_prop_name(name, used_names)
        else:
            props = [e.context_name() for e in self._class_props]
            if prop_names is not None:
                prop_names.extend(props)

        if props and for_iterator:
            props[0:0] = ['@rid']

        return props, lets

    def build_wheres(self, params):
        kw_filters = params.get('kw_filters')
        kw_where = [u' and '.join(u'{0}={1}'
            .format(PropertyEncoder.encode_name(k),
                    ArgConverter.convert_to(ArgConverter.Vertex, v, self))
                for k,v in kw_filters.items())] if kw_filters else []

        filter_exp = params.get('filter')
        exp_where = [self.filter_string(filter_exp)] if filter_exp else []

        return kw_where + exp_where

    def rid_lower(self, skip):
        return '@rid > {}'.format(skip)

    def build_order_expression(self, order_by):
        if isinstance(order_by, tuple):
            return '{} {}'.format(
                ArgConverter.convert_to(ArgConverter.Field, order_by[0], self),
                'DESC' if order_by[1] else 'ASC')
        return ArgConverter.convert_to(ArgConverter.Field, order_by)

    def build_optional_clauses(self, params, skip):
        '''LET, while being an optional clause, must precede WHERE
        and is therefore handled separately.'''
        optional_clauses = []

        group_by = params.get('group_by')
        if group_by:
            group_clause = 'GROUP BY {}'.format(
                ','.join([by.context_name() for by in group_by]))
            optional_clauses.append(group_clause)

        order_by = params.get('order_by')
        if order_by:
            order_clause = 'ORDER BY {0}'.format(
                ','.join([self.build_order_expression(by) for by in order_by]))
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

    @staticmethod
    def unique_prop_name(name, used_names):
        used = used_names.get(name, None)
        if used is None:
            used_names[name] = 1
            return name
        else:
            used_names[name] += 1
            return name + str(used_names[name])

    @staticmethod
    def sanitise_prop_name(name):
        if iskeyword(name):
            return name + '_'
        elif name[0] == '$':
            return 'qv_'+ name[1:]
        else:
            return name

    def build_select(self, props, optional_clauses):
        # This 'is not None' is important; don't want to implicitly call
        # __len__ (which invokes count()) on subquery.
        if self._subquery is not None:
            src = u'({})'.format(self._subquery)
        else:
            src = self.source_name

        optional_string = ' '.join(optional_clauses)
        if props:
            return u'SELECT {}{} {}'.format(
                ','.join(props), (' FROM ' + src) if src else '', optional_string)
        else:
            return u'SELECT FROM {} {}'.format(src, optional_string)

    def parse_record_prop(self, prop):
        if isinstance(prop, list):
            g = self._graph
            # NOTE For 'ridbags', even of length 1, returns a list.
            return g.elements_from_links(prop) if len(prop) > 0 and isinstance(prop[0], OrientRecordLink) else prop
        elif isinstance(prop, OrientRecordLink):
            return self._graph.element_from_link(prop)
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

