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

    @property
    def context(self):
        """Get containing (graph) context"""
        return self._graph

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

class ElementLink(object):
    """Resolves attributes of linked-to elements."""
    def __init__(self, link, cache):
        self._link = link
        self._cache = cache

    def __str__(self):
        return self._link.get_hash()

    def __getattr__(self, name):
        return getattr(self._cache[self._link], name)


from sys import version_info
if version_info[0] < 3:
    next_value = lambda d: next(d.itervalues())
else:
    next_value = lambda d: next(iter(d.values()))

class ElementLinkCollection(object):
    """Resolves linked-to elements in a collection."""
    def __init__(self, collection, cache):
        self._collection = collection
        self._cache = cache

    def __str__(self):
        return str(self._collection)

    def __len__(self):
        return len(self._collection)

    def __contains__(self, item):
        return item in self._collection

    def __getitem__(self, item):
        return self._cache[self._collection[item]]

    def __iter__(self):
        if isinstance(self._collection, dict):
            for k in self._collection:
                # __iter__ expected to yield keys for mapping types
                yield k
        else:
            for link in self._collection:
                yield self._cache[link]

    def itervalues(self):
        """For iterating cached elements from a LinkMap"""
        for link in next_value(self._collection):
            yield self._cache[link]


from collections import Iterable
from pyorient import OrientRecordLink
def decorate_property(prop, cache):
    if isinstance(prop, OrientRecordLink):
        return ElementLink(prop, cache)
    elif isinstance(prop, Iterable):
        link_collection = \
            isinstance(next_value(prop), OrientRecordLink) if isinstance(prop, dict) \
            else isinstance(next(iter(prop)), OrientRecordLink)
        if link_collection:
            return ElementLinkCollection(prop, cache)
    return prop

