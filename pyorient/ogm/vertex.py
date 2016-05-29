from .element import GraphElement
from .broker import VertexBroker

class Vertex(GraphElement):
    Broker = VertexBroker

    # TODO
    # Edge information is carried in vertexes retrieved from database,
    # as OrientBinaryObject. Can likely optimise these traversals
    # when we know how to parse these.
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

    def __call__(self, edge_or_broker):
        """Provides syntactic sugar for creating edges."""
        if hasattr(edge_or_broker, 'broker'):
            edge_or_broker = edge_or_broker.broker.element_cls
        elif hasattr(edge_or_broker, 'element_cls'):
            edge_or_broker = edge_or_broker.element_cls

        if edge_or_broker.decl_type == 1:
            return VertexVector(self, edge_or_broker.objects)

class VertexVector(object):
    def __init__(self, origin, edge_broker, **kwargs):
        self.origin = origin
        self.edge_broker = edge_broker
        self.kwargs = kwargs

    def __gt__(self, target):
        """Syntactic sugar for creating an edge.

        :param target: If a batch variable, return a command for creating an
        edge to this vertex. Otherwise, create the edge.
        """
        if hasattr(target, '_id'):
            if target._id[0] == '$':
                return self.edge_broker.create_command(
                    self.origin, target, **self.kwargs)
            else:
                return self.edge_broker.create(
                    self.origin, target, **self.kwargs)
        return self

