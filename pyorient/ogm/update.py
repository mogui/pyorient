from .element import GraphElement
from .expressions import ExpressionMixin
from .property import PropertyEncoder
from .query_utils import ArgConverter
from .commands import Command

import json

class Update(ExpressionMixin, Command):
    Default = 0
    Record = 1

    Count = 0
    Before = 1
    After = 2

    def __init__(self, graph, entity):
        self._graph = graph
        self._updates = {}
        self._params = {}
        self._edge = None

        from .what import LetVariable

        # TODO Support cluster specification
        if isinstance(entity, GraphElement):
            # Vertex or edge instance
            self._source = entity._id
        elif isinstance(entity, LetVariable):
            self._source = self.build_what(entity)
        else:
            # Vertex or edge class
            self._source = entity.registry_name

    @classmethod
    def edge(cls, graph, entity):
        """Indicate that entity to update is an edge."""
        self = cls(graph, entity)
        self._edge = True
        return self

    def __str__(self):
        if not self._updates:
            # NOTE Not usable as a command, but reflects the current state
            return u'UPDATE ' + self._source

        actions = ''.join([Update.BuildAction._(action_type, self, nvp) for action_type,nvp in self._updates.items()])

        params = self._params

        ret = params.get('return', '')
        if ret:
            ret = u' RETURN {} {}'.format(Update.RETURN_OPS.get(ret[0], ret[0]), self.build_what(ret[1]))

        lock = params.get('lock', None)
        if lock is not None:
            lock = u' LOCK ' + (u'record' if lock else u'default')

        where = params.get('where', '')
        wheres = [self.filter_string(where)] if where else []

        kw_where = params.get('kw_where', {})
        wheres = wheres + ([u' and '.join(u'{}={}'.format(PropertyEncoder.encode_name(k), PropertyEncoder.encode_value(v, self)) for k, v in kw_where.items())] if kw_where else [])

        if wheres:
            where = u' WHERE ' + u' and '.join(wheres)

        limit = params.get('limit', '')
        if limit:
            limit = ' LIMIT ' + str(limit)

        timeout = params.get('timeout', '')
        if timeout:
            timeout = ' TIMEOUT ' + str(timeout)

        # Don't prevent upsert when updating an edge; may be supported by future OrientDB,
        # and when it's not supported, failure should be obvious, not concealed by OGM 
        return u'UPDATE {}{}{}{}{}{}{}{}'.format(
            u'EDGE ' if self._edge else ''
            , self._source
            , actions
            , u' UPSERT' if 'upsert' in params else ''
            , ret
            , where
            , lock if lock is not None else ''
            , limit
            , timeout
                )

    def do(self):
        g = self._graph

        ret = self._params.get('return', Update.Count)
        if ret is Update.Count or ret == 'COUNT':
            return g.client.command(str(self))[0]
        return g.elements_from_records(g.client.command(str(self)))

    def set(self, *nvps):
        """Set field values.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates.pop('json', None)
        self._updates['set'] = nvps
        return self

    def increment(self, *nvps):
        """Increment field values.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates.pop('json', None)
        self._updates['inc'] = nvps
        return self

    def add(self, *nvps):
        """Add to collection.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates.pop('json', None)
        self._updates['add'] = nvps
        return self

    def remove(self, *nvps):
        """Remove from collection or map.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates.pop('json', None)
        self._updates['rem'] = nvps
        return self

    def put(self, *nvps):
        """Put into map.
        :param nvps: Sequence of 2-tuples; first element is the collection
        name, second element a 2-tuple name-value pair
        """
        self._updates.pop('json', None)
        self._updates['put'] = nvps
        return self

    def content(self, json):
        """Replace record content with JSON"""
        self._updates.clear()
        self._updates['json'] = (True, json)
        return self

    def merge(self, json):
        """Merge record content with JSON"""
        self._updates.clear()
        self._updates['json'] = (False, json)
        return self

    def lock(self, strategy):
        self._params['lock'] = strategy
        return self

    def upsert(self, upsert=True):
        self._params['upsert'] = upsert
        return self

    def return_(self, operator, what):
        self._params['return'] = (operator, what)
        return self

    def where(self, condition):
        self._params['where'] = condition
        return self

    def wherein(self, **kwargs):
        self._params['kw_where'] = kwargs
        return self

    def limit(self, max_records):
        self._params['limit'] = max_records
        return self

    def timeout(self, ms):
        self._params['timeout'] = ms
        return self

    RETURN_OPS = {
        Count: 'COUNT'
        , Before: 'BEFORE'
        , After: 'AFTER'
    }

    class BuildAction(object):
        @classmethod
        def _(cls, action, update, spec):
            # Bypass descriptor logic
            return getattr(cls, action)(update, spec)

        @classmethod
        def set(cls, update, spec):
            return ' SET ' + ','.join([cls.eq(nvp[0], nvp[1], update) for nvp in spec])
        @classmethod
        def inc(cls, update, spec):
            return ' INCREMENT ' + ','.join([cls.eq(nvp[0], nvp[1], update) for nvp in spec])
        @classmethod
        def add(cls, update, spec):
            return ' ADD ' + ','.join([cls.eq(nvp[0], nvp[1], update) for nvp in spec])
        @classmethod
        def rem(cls, update, spec):
            # TODO FIXME This won't provide the full range of available behaviours
            return ' REMOVE ' + ','.join([cls.eq(nvp[0], nvp[1], update) for nvp in spec])
        @classmethod
        def put(cls, update, spec):
            return ' PUT ' + ','.join([cls.pair(nvp[0], nvp[1], update) for nvp in spec])

        @classmethod
        def json(cls, update, usage):
            replace = usage[0]
            json = usage[1]

            json = json if isinstance(json, str) else PropertyEncoder.encode_value(json, update)
            if replace:
                return ' CONTENT ' + json
            return ' MERGE ' + json

        @classmethod
        def eq(cls, key, value, update):
            return '{}={}'.format(
                ArgConverter.convert_to(ArgConverter.Field, key, update),
                PropertyEncoder.encode_value(value, update))

        @classmethod
        def pair(cls, collection, nvp, update):
            return '{}={},{}'.format(
                ArgConverter.convert_to(ArgConverter.Field, collection, update),
                json.dumps(nvp[0]), 
                PropertyEncoder.encode_value(nvp[1], update))

