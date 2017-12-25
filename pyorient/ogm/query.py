from .property import Property, PropertyEncoder
from .element import GraphElement
from .exceptions import MultipleResultsFound, NoResultFound
from .query_utils import ArgConverter
from .commands import RetrievalCommand
from .mapping import CacheMixin

from collections import namedtuple
from keyword import iskeyword

import sys
if sys.version < '3':
    import string
    # Sanitises tokens, too
    sanitise_ids = string.maketrans('#:{}', '____')
else:
    sanitise_ids = {
        ord('#'): '_'
        , ord(':'): '_'
        , ord('{'): '_'
        , ord('}'): '_'
    }

class Query(RetrievalCommand, CacheMixin):
    def __init__(self, graph, entities):
        """Query against a class or a selection of its properties.

        :param graph: Graph to query
        :param entities: Vertex/Edge class/a collection of its properties,
        an instance of such a class, or a subquery.
        """
        super(Query, self).__init__()
        self._graph = graph
        self._subquery = None
        self._params = {}
        self._cacher = None # If _cacher None, no _cache, and vice versa
        self._cache = None

        if not entities:
            self.source_name = None
            self._class_props = tuple()
            return

        first_entity = entities[0]

        from .what import What, LetVariable, QT
        from .traverse import Traverse

        if isinstance(first_entity, Property):
            self.source_name = first_entity._context.registry_name
            self._class_props = entities
        elif isinstance(first_entity, GraphElement):
            # Vertex or edge instance
            self.source_name = first_entity._id
            self._class_props = tuple()
        elif isinstance(first_entity, Query):
            # Subquery
            self._subquery = first_entity
            self.source_name = first_entity.source_name
            self._class_props = tuple()
        elif isinstance(first_entity, Traverse):
            self._subquery = first_entity
            self.source_name = None
            self._class_props = tuple()
        elif isinstance(first_entity, (LetVariable, QT)):
            self.source_name = self.build_what(first_entity)
            self._class_props = tuple()
        elif isinstance(first_entity, What):
            self._params['what'] = entities
            self.source_name = None
            self._class_props = tuple()
        else:
            self.source_name = first_entity.registry_name
            self._class_props = tuple(entities[1:])

    @classmethod
    def sub(cls, source):
        """Shorthand for defining a sub-query, which does not need a Graph"""
        return cls(None, (source, ))

    @classmethod
    def proj(cls, *whats):
        """Query without a source (or Graph), just a projection.
        Useful for Batch return values."""
        self = cls(None, None)
        return self.what(*whats)

    @classmethod
    def from_string(cls, command, graph):
        """Create query from pre-written command text.
        :param command: Query command text
        :param graph: Graph instance to traverse
        """
        self = cls(graph, None)
        self._compiled = str(command)
        return self

    def format(self, *args, **kwargs):
        """RetrievalCommand.format() override for Query
        :return: Compiled Query, with tokens replaced, and *shallow* copies of
        parameters to maintain cache and behaviour of projection queries
        Please note that this only does string replacement, it does not replace
        the underlying What.Token instances used for compilation, choosing
        speed at some cost to flexibility.
        """
        encode = self.FORMAT_ENCODER

        new_query = self.from_string(self.compile().format(*[encode(arg) for arg in args], **{k:encode(v) for k,v in kwargs.items()}), self._graph)
        new_query.source_name = self.source_name
        new_query._class_props = self._class_props 
        new_query._params = self._params
        return new_query

    def query(self):
        """Create a query, with current query as a subquery.
        Serves as a useful shorthand for chaining sub-queries."""
        return Query(self._graph, (self, ))

    def traverse(self, *what):
        from .traverse import Traverse
        return Traverse(self._graph, self, *what)

    @property
    def graph(self):
        """Get graph being queried. May be None for subqueries"""
        return self._graph

    @graph.setter
    def graph(self, graph):
        """Set graph being queried"""
        self._graph = graph

    def __iter__(self):
        params = self._params

        # TODO Don't ignore initial skip value
        with TempParams(params, skip='#-1:-1', limit=1):
            optional_clauses, command_suffix = self.build_optional_clauses(params, None)

            prop_names = []
            props, lets = self.build_props(params, prop_names)
            if props and ('what' not in params) or prop_names:
                # Shouldn't be prepended in the case of expand()
                props[0:0] = ['@rid']
            if len(prop_names) > 1:
                prop_prefix = self.source_name.translate(sanitise_ids)

                selectuple = namedtuple(prop_prefix + '_props',
                    [Query.sanitise_prop_name(name)
                        for name in prop_names])
                proj_handler = self._graph.parse_record_prop if params.get('resolve', True) else lambda r, _: r
            elif prop_names:
                proj_handler = self._graph.parse_record_prop if params.get('resolve', True) else lambda r, _: r
            wheres = self.build_wheres(params)

            g = self._graph
            cache = self._cache
            while True:
                current_skip = params['skip']
                where = u'WHERE ' + u' and '.join(
                        [self.rid_lower(current_skip)] + wheres)

                select = self.build_select(props, lets + [where] + optional_clauses)

                response = g.client.command(*((select,) + command_suffix))
                if response:
                    response = response[0]

                    if prop_names:
                        next_skip = response.oRecordData.get('rid', response._rid)
                        if next_skip:
                            self.skip(next_skip)

                            if len(prop_names) > 1:
                                yield selectuple(
                                    *tuple(proj_handler(
                                            response.oRecordData.get(name), cache)
                                        for name in prop_names))
                            else:
                                yield proj_handler(
                                        response.oRecordData[prop_names[0]], cache)
                        else:
                            yield g.element_from_record(response, cache)
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

                        yield g.element_from_record(response, cache)
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
        def compiler():
            props, lets, where, optional_clauses, _ = self.prepare()
            return self.build_select(props, lets + where + optional_clauses)
        return self.compile(compiler)

    def pretty(self):
        """Pretty-print this query, to ease debugging."""
        build_select = self.build_select
        build_lets = self.build_lets
        build_assign_what = self.build_assign_what

        import types
        # TODO FIXME Tweak build_pretty_* functions
        # e.g., clearer distribution of parentheses.
        self.build_select = types.MethodType(build_pretty_select, self)
        self.build_lets = types.MethodType(build_pretty_lets, self)
        self.build_assign_what = types.MethodType(build_pretty_assign_what, self)

        compiled = self._compiled
        self._compiled = None
        prettified = str(self)
        self._compiled = compiled

        self.build_select = build_select
        self.build_lets = build_lets
        self.build_assign_what = build_assign_what
        return prettified

    def __len__(self):
        return self.count()

    def __deepcopy__(self, memo):
        cls = self.__class__
        copy = cls.__new__(cls)
        memo[id(self)] = copy

        copy._graph = self._graph
        copy._subquery = self._subquery
        copy._params = {}
        copy._params.update(self._params)
        copy.source_name = self.source_name
        copy._class_props = self._class_props

        copy._compiled = self._compiled

        return copy

    def prepare(self, prop_names=None):
        params = self._params
        props, lets = self.build_props(params, prop_names)
        skip = params.get('skip')
        if skip and ':' in str(skip):
            rid_clause = [self.rid_lower(skip)]
            skip = None
        else:
            rid_clause = []
        optional_clauses, command_suffix = self.build_optional_clauses(params, skip)

        wheres = rid_clause + self.build_wheres(params)
        where = [u'WHERE ' + u' and '.join(wheres)] if wheres else []

        return props, lets, where, optional_clauses, command_suffix

    def all(self):
        params = self._params
        if self._compiled is not None and 'count' not in params:
            select = self._compiled
            command_suffix = self.build_command_suffix(params.get('limit', None))
            # Must do a little extra work, for projection queries
            prop_names = self.extract_prop_names(params)
        else:
            prop_names = []
            props, lets, where, optional_clauses, command_suffix = self.prepare(prop_names)
            select = self.build_select(props, lets + where + optional_clauses)
            if 'count' not in params:
                self._compiled = select

        if len(prop_names) > 1:
            prop_prefix = self.source_name.translate(sanitise_ids)

            selectuple = namedtuple(prop_prefix + '_props',
                [Query.sanitise_prop_name(name)
                    for name in prop_names])

        g = self._graph
        cache = self._cache

        response = g.client.command(*((select,) + command_suffix))
        if response:
            # TODO Determine which other queries always take only one iteration
            list_query = 'count' not in params

            if list_query:
                if prop_names:
                    proj_handler = self._graph.parse_record_prop if params.get('resolve', True) else lambda r, _: r
                    if len(prop_names) > 1:
                        return [
                            selectuple(*tuple(
                                proj_handler(
                                    record.oRecordData.get(name), cache)
                                for name in prop_names))
                            for record in response]
                    else:
                        prop_name = prop_names[0]
                        return [
                            proj_handler(
                                record.oRecordData[prop_name], cache)
                            for record in response]
                else:
                    if params.get('reify', False) and len(response) == 1:
                        # Simplify query for subsequent uses
                        del params['kw_filters']
                        self.source_name = response[0]._rid

                    return g.elements_from_records(response, cache)
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
            if num_responses == 1:
                return responses[0]
            else:
                if num_responses > 1:
                    raise MultipleResultsFound(
                        'Expecting one result for query; got more.')
                else:
                    raise NoResultFound('Expecting one result for query; got none.')

    def scalar(self):
        try:
            response = self.one()
            return response[0] if isinstance(response, tuple) else response
        except NoResultFound:
            return None

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
        self.purge()
        self._params['what'] = whats
        return self

    def let(self, *ordered, **kwargs):
        """Define LET block for query, setting context variables.
        :param ordered: When context variables have dependencies, order
        matters. In Python < 3.6, OrderedDict will not retain order as kwargs,
        pass them here. See PEP 468.
        :param kwargs: Conveniently specify context variables.
        """
        self.purge()
        if ordered:
            if kwargs is not None:
                ordered[0].update(kwargs)
            self._params['let'] = ordered[0]
        else:
            self._params['let'] = kwargs
        return self

    def filter(self, expression):
        self.purge()
        self._params['filter'] = expression
        return self

    def filter_by(self, **kwargs):
        self.purge()
        self._params['kw_filters'] = kwargs
        return self

    def group_by(self, *criteria):
        self.purge()
        self._params['group_by'] = criteria
        return self

    def order_by(self, *criteria):
        """:param criteria: A projection field, or a 2-tuple of the form
        (<projection field>, <reverse>), where <reverse> is a bool which
        - if True - results in a descending order for the field"""
        self.purge()
        self._params['order_by'] = criteria
        return self

    def unwind(self, field):
        self.purge()
        self._params['unwind'] = field
        return self

    def skip(self, skip):
        self.purge()
        self._params['skip'] = skip
        return self

    def limit(self, limit):
        self.purge()
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
        self.purge()
        self._params['skip'] = start
        if isinstance(start, str):
            self._params['limit'] = stop
        else:
            self._params['limit'] = stop - start
        return self

    def fetch_plan(self, plan, fetch_cache = None):
        """Specify a fetch plan for the query.

        :param plan: A string with a series of space-separated rules of the
        form [[levels]]fieldPath:depthLevel
        :param fetch_cache: A dictionary in which to cache fetched elements,
        indexed by OrientRecordLink. Optional only to avoid extra burden while
        using varied fetch plans in batches. A cache is required for any
        executed command(s) containing fetch plan(s).
        """
        self.purge()
        self._params['fetch'] = plan
        self.cache = fetch_cache
        return self

    def response_options(self, resolve_projections):
        """Fine-tune how responses are processed
        :param resolve_projections: True to resolve links in projection
        queries (the default), False to return projections verbatim
        """
        self._params['resolve'] = resolve_projections
        return self

    def lock(self):
        self.purge()
        self._params['lock'] = True
        return self

    # Internal methods, beyond this point

    def build_props(self, params, prop_names=None):
        lets = self.build_lets(params)

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

        return props, lets

    def build_assign_what(self, k, v):
        return PropertyEncoder.encode_name(k) + u' = ' + \
            (u'(' + str(v) + ')' if isinstance(v, RetrievalCommand) else self.build_what(v))

    def build_assign_vertex(self, k, v):
        return PropertyEncoder.encode_name(k) + u' = ' + \
            ArgConverter.convert_to(ArgConverter.Vertex, v, self)

    def build_lets(self, params):
        let = params.get('let')
        if let:
            return [
                'LET ' + ','.join(
                    self.build_assign_what(k, v)
                    for k,v in let.items())
            ]
        else:
            return []

    def extract_prop_names(self, params):
        whats = params.get('what')
        if whats:
            used_names = {}
            return [self.unique_prop_name(n, used_names)
                        for n in (self.extract_prop_name(what) for what in whats)
                        if n is not None]
        else:
            return [p.context_name() for p in self._class_props]

    def build_wheres(self, params):
        kw_filters = params.get('kw_filters')
        kw_where = [u' and '.join(self.build_assign_vertex(k,v)
                for k,v in kw_filters.items())] if kw_filters else []

        filter_exp = params.get('filter')
        from .what import QT
        if isinstance(filter_exp, QT):
            exp_where = ['{' + filter_exp.token + '}' if filter_exp.token is not None else '{}']
        else:
            exp_where = [self.filter_string(filter_exp)] if filter_exp else []

        return kw_where + exp_where

    def rid_lower(self, skip):
        return '@rid > ' + str(skip)

    def build_order_expression(self, order_by):
        if isinstance(order_by, tuple):
            return ArgConverter.convert_to(ArgConverter.Field, order_by[0], self) + \
                    ' ' + ('DESC' if order_by[1] else 'ASC')
        return ArgConverter.convert_to(ArgConverter.Field, order_by, self)

    def build_command_suffix(self, limit=None):
        if self._cacher:
            # TODO? Add macro to keep synchronised with default CommandMessage _limit
            return (limit or 20, None, self._cacher)
        else:
            return (limit,) if limit else tuple()

    def build_optional_clauses(self, params, skip):
        '''LET, while being an optional clause, must precede WHERE
        and is therefore handled separately.'''
        optional_clauses = []

        group_by = params.get('group_by')
        if group_by:
            group_clause = 'GROUP BY ' + \
                ','.join([by.context_name() for by in group_by])
            optional_clauses.append(group_clause)

        order_by = params.get('order_by')
        if order_by:
            order_clause = 'ORDER BY ' + \
                ','.join([self.build_order_expression(by) for by in order_by])
            optional_clauses.append(order_clause)

        unwind = params.get('unwind')
        if unwind:
           unwind_clause = 'UNWIND ' + (unwind.context_name() if isinstance(unwind, Property) else unwind)
           optional_clauses.append(unwind_clause)

        if skip:
            optional_clauses.append('SKIP ' + str(skip))

        limit = None
        if 'count' not in params: # TODO Determine other functions for which limit is useless
            limit = params.get('limit')
            if limit:
                optional_clauses.append('LIMIT ' + str(limit))

        fetch = params.get('fetch')
        if fetch:
            optional_clauses.append('FETCHPLAN ' + fetch)

        lock = params.get('lock')
        if lock:
            optional_clauses.append('LOCK RECORD')

        return optional_clauses, self.build_command_suffix(limit)

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
            src = u'(' + str(self._subquery) + ')'
        else:
            src = self.source_name

        optional_string = ' '.join(optional_clauses)
        if props:
            return u'SELECT ' + ','.join(props) + \
                    ((' FROM ' + src) if src else '') + ' ' + optional_string
        else:
            return u'SELECT FROM ' + src + ' ' + optional_string

def build_pretty_select(self, props, optional_clauses):
    query_spaces = self._params.get('indent', 0)
    query_idt = ' ' * query_spaces
    prop_spaces = 7 + query_spaces
    prop_idt = ' ' * prop_spaces

    clause_spaces = 4 + query_spaces
    idt = ' ' * clause_spaces
    new_idt = '\n' + idt

    if self._subquery is not None:
        subq = self._subquery
        subq._params['indent'] = query_spaces + 8
        src = u'(\n' + self._subquery.pretty() + new_idt + ')'
    else:
        src = self.source_name

    optional_string = (new_idt).join(optional_clauses)
    optional_string = (new_idt + optional_string if optional_string else '')
    if props:
        from_src = (new_idt + 'FROM ' + src) if src else ''
        if len(props) > 1:
            prop_divider = '\n' + prop_idt + ', '
            return query_idt + u'SELECT ' + props[0] + prop_divider + prop_divider.join(props[1:]) + \
                from_src + optional_string
        else:
            return query_idt + u'SELECT ' + props[0] + from_src + optional_string
    else:
        return query_idt + u'SELECT FROM ' + src + optional_string

def build_pretty_lets(self, params):
    prefix_spaces = 8 + self._params.get('indent', 0)
    idt = ' ' * prefix_spaces

    let = params.get('let')
    if let:
        lets = iter(let.items())
        k, v = next(lets)
        let_divider = '\n' + idt + ', '
        if len(let) > 1:
            return [
                'LET ' + self.build_assign_what(k, v) + let_divider + let_divider.join(self.build_assign_what(k,v) for k,v in lets)
            ]
        else:
            return [
                'LET ' + self.build_assign_what(k, v)
            ]
    else:
        return []

def build_pretty_assign_what(self, k, v):
    name = PropertyEncoder.encode_name(k)
    if isinstance(v, RetrievalCommand):
        v._params['indent'] = self._params.get('indent', 0) + len(name) + 14
        val = u'(' + v.pretty().strip(' ') + ')'
    else:
        val = self.build_what(v)
    return name + u' = ' + val

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

