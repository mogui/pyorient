from .element import GraphElement
from .broker import EdgeBroker

class Edge(GraphElement):
    Broker = EdgeBroker

    def __init__(self, **kwargs):
        super(Edge, self).__init__(**kwargs)

        self._in = None
        self._out = None

    @classmethod
    def from_graph(cls, graph, element_id, in_hash, out_hash, props):
        edge = super(Edge, cls).from_graph(graph, element_id, props);
        edge._in = in_hash
        edge._out = out_hash

        return edge

    def outV(self):
        g = self._graph
        return g.get_vertex(self._out) if g else None

    def inV(self):
        g = self._graph
        return g.get_vertex(self._in) if g else None

