import pyorient.ogm.property

class GraphElement(object):
    def __init__(self, **kwargs):
        self._graph = None
        self._id = None

        self._props = kwargs

    @classmethod
    def from_graph(cls, graph, element_id, props):
        elem = cls(**props)

        elem._graph = graph
        elem._id = element_id

        return elem

    def save(self):
        """:returns: True if successful, False otherwise"""
        if not self._graph:
            raise RuntimeError(
                'Can not save() element: it has no corresponding Graph')
        return self._graph.save_element(self.__class__, self._props, self._id)

    def __setattr__(self, key, value):
        # Check if the attribute is actually a property of the OGM type
        if (hasattr(type(self), key) and
            isinstance(getattr(type(self), key), pyorient.ogm.property.Property)):
                self._props[key] = value
                return

        super(GraphElement, self).__setattr__(key, value)

    def __getattribute__(self, key):
        try:
            return super(GraphElement, self).__getattribute__('_props')[key]
        except:
            attr = super(GraphElement, self).__getattribute__(key)
            # Make sure to never return properties as instance attributes
            if isinstance(attr, pyorient.ogm.property.Property):
                return None
            else:
                return attr

    def __eq__(self, other):
        return type(self) is type(other) and \
               self._id == other._id and \
               self._props == other._props

    def __ne__(self, other):
        return not self.__eq__(other)

    def __format__(self, format_spec):
        """Quoted record id for specifying element as string argument."""
        return repr(self._id)

