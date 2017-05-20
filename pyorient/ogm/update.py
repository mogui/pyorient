from .element import GraphElement
from .expressions import ExpressionMixin
from .property import PropertyEncoder
from .query_utils import ArgConverter
from .commands import Command


from collections import defaultdict

class Update(ExpressionMixin, Command):
    Default = 0
    Record = 1

    Count = 0
    Before = 1
    After = 2

    def __init__(self, graph, entity):
        self._graph = graph
        # Single update may be of only one kind
        self._updates = (None, None)
        self._params = {}
        self._edge = None

        # TODO Support cluster specification
        if isinstance(entity, GraphElement):
            # Vertex or edge instance
            self._source = entity._id
        else:
            # Vertex or edge class
            self._source = entity.registry_name

    @classmethod
    def edge(cls, graph, entity):
        """Indicate that entity to update is an edge."""
        self = cls.__init__(graph, entity)
        self._edge = True
        return self

    def __str__(self):
        if self._updates[0] is None:
            return u'UPDATE ' + self._source

        actions = Update.BuildAction._(self._updates[0], self, self._updates[1])

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

        return u'UPDATE {}{}{}{}{}{}{}{}'.format(
            u'EDGE ' if self._edge else ''
            , self._source
            , actions
            , u' UPSERT' if 'upsert' in params and not self._edge else ''
            , ret
            , where
            , lock if lock is not None else ''
            , u' LIMIT {}'.format(params['limit']) if 'limit' in params else ''
            , u' TIMEOUT {}'.format(params['timeout']) if 'timeout' in params else ''
                )

    def do(self):
        g = self._graph
        response = g.client.command(str(self))

    def set(self, *nvps):
        """Set field values.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates = ('set', nvps)
        return self

    def increment(self, *nvps):
        """Increment field values.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates = ('inc', nvps)
        return self

    def add(self, *nvps):
        """Add to collection.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates = ('add', nvps)
        return self

    def remove(self, *nvps):
        """Remove from collection or map.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates = ('rem', nvps)
        return self

    def put(self, *nvps):
        """Put into map.
        :param nvps: Sequence of 2-tuple name-value pairs
        """
        self._updates = ('put', nvps)
        return self

    def content(self, json):
        """Replace record content with JSON"""
        self._updates = ('json', (True, json))
        return self

    def merge(self, json):
        """Replace record content with JSON"""
        self._updates = ('json', (False, json))
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
            return ' REMOVE ' + ','.join([cls.eq(nvp[0], nvp[1], update) for nvp in spec])
        @classmethod
        def put(cls, update, spec):
            return ' PUT ' + ','.join([cls.eq(nvp[0], nvp[1], update) for nvp in spec])

        @classmethod
        def json(cls, update, usage):
            replace = usage[0]

            if replace:
                return ' CONTENT ' + PropertyEncoder.encode_value(json, update)
            return ' MERGE ' + PropertyEncoder.encode_value(json, update)

        @classmethod
        def eq(cls, key, value, update):
            return '{}={}'.format(
                ArgConverter.convert_to(ArgConverter.Field, key, update),
                PropertyEncoder.encode_value(value, update))

