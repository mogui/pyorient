from .element import GraphElement
from .expressions import ExpressionMixin
from .property import PropertyEncoder
from .query_utils import ArgConverter


from collections import defaultdict

class Update(ExpressionMixin):
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
            return 'UPDATE ' + self._source

        actions = Update.BuildAction._(self._updates[0], self, self._updates[1])

        ret = self._params.get('return', '')
        if ret:
            ret = ' RETURN {} {}'.format(Update.RETURN_OPS.get(ret[0], ret[0]), self.build_what(ret[1]))

        lock = self._params.get('lock', None)
        if lock is not None:
            lock = ' LOCK ' + ('record' if lock else 'default')

        where = self._params.get('where', '')
        if where:
            where = ' WHERE ' + self.filter_string(where)

        return 'UPDATE {}{}{}{}{}{}{}{}'.format(
            'EDGE ' if self._edge else ''
            , self._source
            , actions
            , ' UPSERT' if 'upsert' in self._params and not self._edge else ''
            , ret
            , where
            , lock if lock is not None else ''
            , ' LIMIT {}'.format(self._params['limit']) if 'limit' in self._params else ''
            , ' TIMEOUT {}'.format(self._params['timeout']) if 'timeout' in self._params else ''
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
            return ' SET ' + ','.join([cls.eq(nvp[0], nvp[1]) for nvp in spec])
        @classmethod
        def inc(cls, update, spec):
            return ' INCREMENT ' + ','.join([cls.eq(nvp[0], nvp[1]) for nvp in spec])
        @classmethod
        def add(cls, update, spec):
            return ' ADD ' + ','.join([cls.eq(nvp[0], nvp[1]) for nvp in spec])
        @classmethod
        def rem(cls, update, spec):
            return ' REMOVE ' + ','.join([cls.eq(nvp[0], nvp[1]) for nvp in spec])
        @classmethod
        def put(cls, update, spec):
            return ' PUT ' + ','.join([cls.eq(nvp[0], nvp[1]) for nvp in spec])

        @classmethod
        def json(cls, update, usage):
            replace = usage[0]

            if replace:
                return ' CONTENT ' + PropertyEncoder.encode_value(json)
            return ' MERGE ' + PropertyEncoder.encode_value(json)

        @classmethod
        def eq(cls, key, value):
            return '{}={}'.format(
                ArgConverter.convert_to(ArgConverter.Field, key, None),
                PropertyEncoder.encode_value(value))

