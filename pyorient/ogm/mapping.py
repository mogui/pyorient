from collections import Iterable
from pyorient import OrientRecordLink

class MapperConfig(object):
    """Configure the Object-Graph Mapper"""
    def __init__(self, strict=False, decoration=None):
        """:param kwargs: 
        strict (bool, default False): Strict property checking. Raise
        AttributeError when class lacks expected property.
        decoration: There are two variants of dynamic link resolution.
        Decorate.Elements will have graph elements (Vertex or Edge) resolve Link
        properties as they are accessed.
        Decorate.Properties will decorate Link properties to handle their own
        resolving.
        Unless Decorate.Nothing (the default) is specified, link collections
        will always be decorated to handle their own resolving.
        """
        self.strict = strict
        self.decorate = decoration

is_link_collection = \
    lambda prop: isinstance(next_value(prop), OrientRecordLink) if isinstance(prop, dict) \
        else isinstance(next(iter(prop)), OrientRecordLink)

class Decorate(object):
    Nothing = None  # Handling links and link collections is up to you
    Elements = 1    # Light, obscures Link properties by pretending they are what they link to
    Properties = 2  # Heavy, but allows easy Link access.

    @staticmethod
    def property(prop, cache):
        """Decorate links to handle their own resolution"""
        if isinstance(prop, OrientRecordLink):
            return ElementLink(prop, cache)
        elif isinstance(prop, Iterable) and is_link_collection(prop):
            return ElementLinkCollection(prop, cache)
        return prop

    @staticmethod
    def iterable(prop, cache):
        """Decorate only link collections, other links must be resolved by containing class"""
        if isinstance(prop, Iterable) and is_link_collection(prop):
            return ElementLinkCollection(prop, cache)
        return prop

def create_cache_callback(graph, cache):
    if cache is None:
        return None

    def cache_cb(record):
        cache[OrientRecordLink(record._rid[1:])] = graph.element_from_record(record, cache)
    return cache_cb

class CacheMixin(object):
    """Manages a cache dictionary and the callback for adding to it
    Assumes inheriting classes specify:
        - 'graph' variable or property
    """
    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        self._cacher = create_cache_callback(self.graph, cache)
        self._cache = cache

    @property
    def cacher(self):
        return self._cacher

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

