from pyorient import OrientRecordLink
import pyorient.ogm.property

class GraphElement(object):
    def __init__(self, **kwargs):
        self._graph = None
        self._id = None

        self._props = GraphElement.PropertyLookup(self, kwargs)

    class PropertyLookup(dict):
        def __init__(self, elem, props):
            dict.__init__(self, props)
            self._elem = elem
            self.lookup_op = lambda v: v

        def __getitem__(self, key):
            return self.lookup_op(dict.__getitem__(self, key)) 

        def __missing__(self, key):
            attr = object.__getattribute__(self._elem, key)
            # Make sure to never return properties as instance attributes
            if isinstance(attr, pyorient.ogm.property.Property):
                return None
            else:
                return attr

    @classmethod
    def from_graph(cls, graph, element_id, props, cache=None):
        elem = cls(**props)

        elem._graph = graph
        elem._id = element_id

        if cache is not None:
            # TODO Consider strategies (and practical value) to replace links
            # with resolved elements, to avoid repeated resolving
            # Seems like premature optimisation...
            def cache_lookup(v):
                if isinstance(v, OrientRecordLink):
                    return cache.get(v,v)
                else:
                    return v
            elem._props.lookup_op = cache_lookup
        return elem

    @property
    def context(self):
        """Get containing (graph) context"""
        return self._graph

    def load(self, cache=None):
        """(Re)populate element, retrieving data from graph"""
        self._props.update(self._graph.load_element(self.__class__, self._id, cache))
        return self

    def save(self):
        """:return: True if successful, False otherwise"""
        if not self._graph:
            raise RuntimeError(
                'Can not save() element: it has no corresponding Graph')
        return self._graph.save_element(self.__class__, self._props, self._id)

    def query(self):
        return self._graph.query(self)

    def traverse(self, *what):
        return self._graph.traverse(self, *what)

    def update(self):
        return self._graph.update(self)

    def __setattr__(self, key, value):
        # Check if the attribute is actually a property of the OGM type
        if (hasattr(type(self), key) and
                isinstance(getattr(type(self), key), pyorient.ogm.property.Property)):
            self._props[key] = value
            return

        super(GraphElement, self).__setattr__(key, value)

    def __getattribute__(self, key):
        return object.__getattribute__(self, '_props')[key]

    def __eq__(self, other):
        return self._id == other._id and \
               self._props == other._props and \
               type(self) is type(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __format__(self, format_spec):
        """Quoted record id for specifying element as string argument."""
        return repr(self._id)

