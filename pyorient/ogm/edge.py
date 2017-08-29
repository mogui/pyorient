from .element import GraphElement
from .broker import EdgeBroker
from .what import What, ChainableWhat

class Edge(GraphElement):
    Broker = EdgeBroker

    def __init__(self, **kwargs):
        super(Edge, self).__init__(**kwargs)

        self._in = None
        self._out = None

    @classmethod
    def from_graph(cls, graph, element_id, in_hash, out_hash, props, cache=None):
        edge = super(Edge, cls).from_graph(graph, element_id, props, cache)
        edge._in = in_hash
        edge._out = out_hash

        return edge

    def load(self, cache=None):
        loaded = self._graph.load_edge(self.__class__, self._id, cache)
        if loaded is not None:
            self._in, self._out, props = loaded
            self._props.update(props)
        return self

    def outV(self):
        g = self._graph
        return g.get_vertex(self._out) if g else None

    def inV(self):
        g = self._graph
        return g.get_vertex(self._in) if g else None

    # TODO To prevent subclasses from getting precedence in equality
    # comparisons, and hence for more intuitive SQL commands, these may be
    # better with another ChainableWhat subclass. Consider this more
    # seriously if people raise issues.
    in_ = ChainableWhat([], [(What.EdgeIn, )])
    out = ChainableWhat([], [(What.EdgeOut, )])

