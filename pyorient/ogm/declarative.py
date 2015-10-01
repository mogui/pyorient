from .vertex import Vertex
from .edge import Edge
from .property import Property

#from enum import Enum
from collections import OrderedDict

class DeclarativeMeta(type):
    """Metaclass for registering node and relationship types.

    Node and relationship metadata is mostly ignored until their classes are
    created in a Graph. The main benefit is to allow 'self-referencing'
    properties with LinkedClassProperty subclasses.
    """
    def __init__(cls, class_name, bases, attrs):
        if not hasattr(cls, 'registry'):
            cls.registry = OrderedDict()
            cls.decl_root = cls
        else:
            decl_bases = set(
                base.decl_root for base in bases
                    if hasattr(base, 'decl_root') and base is not base.decl_root)
            if len(decl_bases) > 1:
                raise TypeError(
                    'When multiply-inheriting graph elements, they must share '
                    'the same declarative base class. '
                    'Note: Each call to declarative_*() returns a new base class.')

            if cls.decl_type is DeclarativeType.Vertex:
                cls.registry_name = attrs.get('element_type'
                                              , cls.__name__.lower())

                plural = attrs.get('element_plural')
                if plural:
                    cls.registry_plural = plural
            else:
                label = attrs.get('label')
                if label:
                    cls.registry_name = cls.registry_plural = label
                else:
                    cls.registry_name = cls.__name__.lower()

            # See also __setattr__ for properties added after class definition
            for prop in cls.__dict__.values():
                if not isinstance(prop, Property):
                    continue
                prop._context = cls

            # FIXME Only want bases that correspond to vertex/edge classes.
            cls.registry[cls.registry_name] = cls

        return super(DeclarativeMeta, cls).__init__(class_name, bases, attrs)

    def __setattr__(self, name, value):
        if isinstance(value, Property):
            if value.context:
                raise ValueError(
                    'Attempt to add a single Property to multiple classes.')
            value.context = self
        return super(DeclarativeMeta, self).__setattr__(name, value)

    def __format__(self, format_spec):
        """Quoted class-name for specifying class as string argument.

        Use 'registry_name' when it is possible to refer to schema entities
        directly.
        """
        return repr(self.registry_name)

# Enum only in Python >= 3.4
#class DeclarativeType(Enum):
class DeclarativeType(object):
    """Marker for graph database element types"""
    Vertex = 0
    Edge = 1

def declarative_base(decl_type, name, cls, metaclass, **kwargs):
    """Create base class for defining new database classes.

    :param decl_type: DeclarativeType enum value.
    :param name: Metaclass name
    :param cls: Base class(es) for returned class
    :param metaclass: Metaclass for registering type information
    """
    bases = cls if isinstance(cls, tuple) else (cls, )
    class_dict = dict(decl_type=decl_type)
    class_dict.update(kwargs)

    return metaclass(name, bases, class_dict)

def declarative_node(name='Node', cls=Vertex, metaclass=DeclarativeMeta
                     , **kwargs):
    """Create base class for graph nodes/vertexes"""
    return declarative_base(DeclarativeType.Vertex, name, cls, metaclass
                            , **kwargs)

def declarative_relationship(name='Relationship', cls=Edge
                             , metaclass=DeclarativeMeta
                             , **kwargs):
    """Create base class for graph relationships/edges

    If no_graph_broker=True is supplied as a keyword argument, the Graph
    will not attach labelled subclass's Broker instances.
    """
    return declarative_base(DeclarativeType.Edge, name, cls, metaclass
                            , **kwargs)

