from .operators import LogicalConnective

class Broker(object):
    """All node and relationship classes registered with a Graph have a
    corresponding Broker, to provide a shorthand for dealing with elements
    of those types.

    How these shorthands are exposed is customizable.

    By default, node and relationship classes will, upon registration with
    a Graph (via create_all() or include()), have an 'objects' attribute
    set to that class's Broker.

    For node classes with an 'element_plural' string, this same Broker
    will also by attached to the Graph, via that string.

        e.g. class Foo(Node):
                element_plural = 'foos'
                name = String()

             g.include(Node.registry)
             g.foos.create(name='Bar')

    Similarly for relationship classes with a 'label' attribute.

    These attributes will be necessary if you intend to use the same node or
    relationship class between two Graphs.

    Custom brokers can be set in various ways for a node or relationship class.
    By setting the 'Broker' attribute of that class to your custom type, and/or
    by assigning your custom Broker type instance to another attribute (if you
    want to use 'objects' as the name of a Property, for example).
    """
    def __init__(self, g=None, element_cls=None):
        self.g = g
        self.element_cls = element_cls

    def init(self, g, element_cls):
        """Associate broker with a Graph and a vertex/edge class."""
        self.g = g
        self.element_cls = element_cls

    def query(self, *entities, **filter_by):
        if entities and isinstance(entities[0], LogicalConnective):
            q = self.g.query(self.element_cls)
            return q.filter(entities[0]).filter_by(**filter_by)
        else:
            return self.g.query(self.element_cls, *entities).\
                        filter_by(**filter_by)

    def query_command(self, *entities, **filter_by):
        return self.query(*entities, **filter_by)

class VertexBroker(Broker):
    def create(self, **kwargs):
        return self.g.create_vertex(self.element_cls, **kwargs)

    def create_command(self, **kwargs):
        return self.g.create_vertex_command(self.element_cls, **kwargs)

class EdgeBroker(Broker):
    def create(self, from_vertex, to_vertex, **kwargs):
        return self.g.create_edge(
            self.element_cls, from_vertex, to_vertex, **kwargs)

    def create_command(self, from_vertex, to_vertex, **kwargs):
        return self.g.create_edge_command(
            self.element_cls, from_vertex, to_vertex, **kwargs)

def get_broker(cls):
    for v in cls.__dict__.values():
        if isinstance(v, Broker):
            return v
    return None

