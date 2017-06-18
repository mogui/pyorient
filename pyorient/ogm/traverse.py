from .expressions import ExpressionMixin
from .commands import Command
from .query_utils import ArgConverter

class Traverse(ExpressionMixin, Command):
    DepthFirst = 0
    BreadthFirst = 1

    def __init__(self, graph, target, *what):
        self._graph = graph
        self._target = target
        self._what = what
        self._params = {}

    def query(self):
        from .query import Query
        return Query(self._graph, (self, ))

    def maxdepth(self, depth):
        self._params['pred'] = (False, depth)
        return self

    def while_(self, condition):
        self._params['pred'] = (True, condition)
        return self

    def limit(self, limit):
        self._params['limit'] = limit
        return self

    def depth_first(self):
        self._params['strategy'] = Traverse.DepthFirst
        return self

    def breadth_first(self):
        self._params['strategy'] = Traverse.BreadthFirst
        return self

    TEMPLATE = u'TRAVERSE {} FROM {}{}{}{}'
    def __str__(self):
        what = self.build_fields(tuple())
        predicate, limit, strategy = self.build_optional()

        from .query import Query
        return Traverse.TEMPLATE.format(what,
            u'({})'.format(self._target) if isinstance(self._target, Query)
                else ArgConverter.convert_to(ArgConverter.Vertex, self._target, self),
            predicate, limit, strategy)

    def all(self, *what):
        g = self._graph

        what = self.build_fields(what)
        predicate, limit, strategy = self.build_optional()

        traverse = Traverse.TEMPLATE.format(what, ArgConverter.convert_to(ArgConverter.Vertex, self._target, self), predicate, limit, strategy)

        response = g.client.command(traverse)
        if response:
            return g.elements_from_records(response)

        return []

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

