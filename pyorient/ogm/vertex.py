from .element import GraphElement
from .broker import VertexBroker

class Vertex(GraphElement):
    Broker = VertexBroker

    def outE(self, *edge_classes):
        g = self._graph
        return g.outE(self._id, *edge_classes) if g else None

    def inE(self, *edge_classes):
        g = self._graph
        return g.inE(self._id, *edge_classes) if g else None

    def bothE(self, *edge_classes):
        g = self._graph
        return g.bothE(self._id, *edge_classes) if g else None

    def out(self, *edge_classes):
        g = self._graph
        return g.out(self._id, *edge_classes) if g else None

    def in_(self, *edge_classes):
        g = self._graph
        return g.in_(self._id, *edge_classes) if g else None

    def both(self, *edge_classes):
        g = self._graph
        return g.both(self._id, *edge_classes) if g else None

