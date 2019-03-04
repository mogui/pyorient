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

    @property
    def graph(self):
        """Get graph being queried. May be None for subqueries"""
        return self._graph

    @graph.setter
    def graph(self, graph):
        """Set graph being queried"""
        self._graph = graph

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

    def __str__(self):
        return self.compile(self.build_compiler())

    def pretty(self):
        build_compiler = self.build_compiler

        import types
        # TODO FIXME Currently this only indents the entire traverse, to play
        # nicely with Query.pretty(). Might want to split traverse string across
        # lines, if it seems useful enough
        self.build_compiler = types.MethodType(build_pretty_compiler, self)

        compiled = self._compiled
        self._compiled = None
        prettified = str(self)
        self._compiled = compiled

        self.build_compiler = build_compiler

        return prettified

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

    TEMPLATE = u'TRAVERSE {} FROM {}{}{}{}'
    def build_compiler(self):
        from .query import Query
        return lambda: Traverse.TEMPLATE.format(self.build_fields(tuple()),
            u'(' + str(self._target) + ')' if isinstance(self._target, Query)
                else ArgConverter.convert_to(ArgConverter.Vertex, self._target, self),
            *self.build_optional())

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


def build_pretty_compiler(self):
    """:return: String compiler for Traverse.pretty()"""
    from .query import Query
    def compiler():
        traverse_spaces = self._params.get('indent', 0)
        traverse_idt = ' ' * traverse_spaces
        target = self._target
        return traverse_idt + Traverse.TEMPLATE.format(self.build_fields(tuple()),
            u'(' + target.pretty() + ')' if isinstance(target, Query)
                else ArgConverter.convert_to(ArgConverter.Vertex, target, self),
            *self.build_optional())
    return compiler

