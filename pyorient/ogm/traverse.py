from .commands import RetrievalCommand
from .query_utils import ArgConverter

import warnings

class Traverse(RetrievalCommand):
    DepthFirst = 0
    BreadthFirst = 1

    def __init__(self, graph, target, *what, **kwargs):
        """Traverse a graph from a given target.

        :param graph: Graph to traverse
        :param target: Target from which to begin traversal
        :param what: Fields to traverse, or any()/all()
        :param kwargs: 'cache', to enable linked properties
        of traversed elements to be dynamically resolved,
        provided a cache."""
        super(Traverse, self).__init__()
        self._graph = graph
        self._target = target
        self._what = what
        self._params = {}
        self._cache = kwargs.get('cache', None)

    def __deepcopy__(self, memo):
        cls = self.__class__
        copy = cls.__new__(cls)
        memo[id(self)] = copy

        copy._graph = self._graph
        copy._target = self._target
        copy._what = self._what
        copy._params = {}
        copy._params.update(self._params)
        copy._cache = self._cache

        copy._compiled = self._compiled

        return copy

    @classmethod
    def from_string(cls, command, graph, **kwargs):
        """Create traversal from pre-written command text.
        :param command: Traverse command text
        :param graph: Graph instance to traverse
        """
        self = cls(graph, None, **kwargs)
        self._compiled = str(command)
        return self

    def format(self, *args, **kwargs):
        """RetrievalCommand.format() override for Traverse
        :return: Compiled Traverse, with tokens replaced, and fields/parameters discarded
        Please note that this only does string replacement, it does not replace
        the underlying What.Token instances used for compilation, choosing
        speed at some cost to flexibility.
        """
        encode = self.FORMAT_ENCODER
        new_traverse = self.from_string(self.compile().format(*[encode(arg) for arg in args], **{k:encode(v) for k,v in kwargs.items()}), self._graph)
        new_traverse._target = self._target
        new_traverse._cache = self._cache
        return new_traverse

    def query(self):
        from .query import Query
        return Query(self._graph, (self, ))

    def maxdepth(self, depth):
        self.purge()
        self._params['pred'] = (False, depth)
        return self

    def while_(self, condition):
        self.purge()
        self._params['pred'] = (True, condition)
        return self

    def limit(self, limit):
        self.purge()
        self._params['limit'] = limit
        return self

    def depth_first(self):
        self.purge()
        self._params['strategy'] = Traverse.DepthFirst
        return self

    def breadth_first(self):
        self.purge()
        self._params['strategy'] = Traverse.BreadthFirst
        return self

    TEMPLATE = u'TRAVERSE {} FROM {}{}{}{}'
    def __str__(self):
        from .query import Query
        return self.compile(lambda: Traverse.TEMPLATE.format(self.build_fields(tuple()),
            u'({})'.format(self._target) if isinstance(self._target, Query)
                else ArgConverter.convert_to(ArgConverter.Vertex, self._target, self),
            *self.build_optional()))

    def all(self, *what):
        if self._compiled is not None and self.avoid_compile(what):
            traverse = self._compiled
        else:
            what = self.build_fields(what)
            predicate, limit, strategy = self.build_optional()
            traverse = self._compiled = Traverse.TEMPLATE.format(what, ArgConverter.convert_to(ArgConverter.Vertex, self._target, self), predicate, limit, strategy)

        g = self._graph
        response = g.client.command(traverse)
        if response:
            return g.elements_from_records(response, self._cache)

        return []

    def avoid_compile(self, what):
        """Check if (re)compile can be avoided.
        :param what: Tuple of arguments passed to all()
        """
        if what:
            if self._target:
                warnings.warn('Arguments to all() forcing recompile; check if required.', RuntimeWarning)
                return False
            else:
                warnings.warn('Arguments to all() ignored. If pre-written commands should vary, pre-write variants.', SyntaxWarning)
        return True

    def build_fields(self, what):
        if not what:
            what = self._what
            if not what:
                from .what import all
                what = (all(), )
        return ','.join([self.build_what(w) for w in what])

    def build_optional(self):
        predicate = self._params.get('pred', '')
        if predicate:
            if predicate[0]:
                predicate = ' {} {}'.format('WHILE', self.filter_string(predicate[1]))
            else:
                predicate = ' {} {}'.format('MAXDEPTH', predicate[1])

        limit = self._params.get('limit', '')
        if limit:
            limit = ' LIMIT {}'.format(limit)

        strategy = self._params.get('strategy', '')
        if strategy:
            strategy = ' STRATEGY {}'.format('BREADTH_FIRST' if strategy is Traverse.BreadthFirst else 'DEPTH_FIRST')

        return predicate, limit, strategy

