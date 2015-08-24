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

    def outV(self, *edge_classes):
        g = self._graph
        return g.outV(self._id, *edge_classes) if g else None

    def inV(self, *edge_classes):
        g = self._graph
        return g.inV(self._id, *edge_classes) if g else None

    def bothV(self, *edge_classes):
        g = self._graph
        return g.bothV(self._id, *edge_classes) if g else None

