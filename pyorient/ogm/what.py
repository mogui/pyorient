from pyorient.ogm.operators import (IdentityOperand, RelativeOperand, Operand, InstanceOfMixin, ArithmeticMixin)
from pyorient.ogm.query_utils import ArgConverter

class What(object):
    """Specify 'what' a Query retrieves."""
    Out = 0
    In = 1
    Both = 2
    OutE = 3
    InE = 4
    BothE = 5
    OutV = 6
    InV = 7
    Eval = 8
    Coalesce = 9
    If = 10
    IfNull = 11
    Expand = 12
    First = 13
    Last = 14
    Count = 15
    Min = 16
    Max = 17
    Abs = 18
    Avg = 19
    Mode = 20
    Median = 21
    Percentile = 22
    Variance = 23
    StdDev = 24
    Sum = 25
    Date = 26
    SysDate = 27
    Format = 28
    AStar = 29
    Dijkstra = 30
    ShortestPath = 31
    Distance = 32
    Distinct = 33
    UnionAll = 34
    Intersect = 35
    Difference = 36
    SymmetricDifference = 37
    Set = 38
    List = 39
    Map = 40
    TraversedElement = 41
    TraversedEdge = 42
    TraversedVertex = 43
    Any = 44
    All = 45
    # Methods
    Subscript = 46
    Append = 47
    AsBoolean = 48
    AsDate = 49
    AsDatetime = 50
    AsDecimal = 51
    AsFloat = 52
    AsInteger = 53
    AsList = 54
    AsLong = 55
    AsMap = 56
    AsSet = 57
    AsString = 58
    CharAt = 59
    Convert = 60
    Exclude = 61
    FormatMethod = 62
    Hash = 63
    Include = 64
    IndexOf = 65
    JavaType = 66
    Keys = 67
    Left = 68
    Length = 69
    Normalize = 70
    Prefix = 71
    Remove = 72
    RemoveAll = 73
    Replace = 74
    Right = 75
    Size = 76
    SubString = 77
    Trim = 78
    ToJSON = 79
    ToLowerCase = 80
    ToUpperCase = 81
    Type = 82
    Values = 83
    # Filter
    WhatFilter = 84
    # Custom functions
    WhatCustom = 85
    # Let
    WhatLet = 86
    # Standard Graph classes and properties
    VertexClass = 87
    EdgeClass = 88
    EdgeIn = 89
    EdgeOut = 90
    # Record attributes
    AtThis = 91
    AtRid = 92
    AtClass = 93
    AtVersion = 94
    AtSize = 95
    AtType = 96
    # Sequences
    Sequence = 97
    Current = 98
    Next = 99

    def __init__(self):
        self.name_override = None

    def as_(self, name_override):
        self.name_override = name_override
        return self

def eval(exp):
    return FunctionWhat(What.Eval, (exp,))

def coalesce(*params):
    return FunctionWhat(What.Coalesce, params)

def if_(cond, con, alt):
    return FunctionWhat(What.If, (cond, con, alt))

def ifnull(field, value):
    return FunctionWhat(What.IfNull, (field, value))

def expand(field):
    return FunctionWhat(What.Expand, (field,))

def first(field):
    return FunctionWhat(What.First, (field,))

def last(field):
    return FunctionWhat(What.Last, (field,))

def count(field):
    return FunctionWhat(What.Count, (field,))

def min(field, *more):
    return FunctionWhat(What.Min, [field] + [f for f in more])

def max(field, *more):
    return FunctionWhat(What.Max, [field] + [f for f in more])

def abs(field):
    return FunctionWhat(What.Abs, (field,))

def avg(field):
    return FunctionWhat(What.Avg, (field,))

def mode(field):
    return FunctionWhat(What.Mode, (field,))

def median(field):
    return FunctionWhat(What.Median, (field,))

def percentile(field, *quantiles):
    return FunctionWhat(What.Percentile, (field, ) + quantiles)

def variance(field):
    return FunctionWhat(What.Variance, (field,))

def stddev(field):
    return FunctionWhat(What.StdDev, (field,))

def sum(field):
    return FunctionWhat(What.Sum, (field,))

def date(date_str, fmt=None, tz=None):
    return FunctionWhat(What.Date, (date_str, fmt, tz))

def sysdate(fmt=None, tz=None):
    return FunctionWhat(What.SysDate, (fmt, tz))

def format(fmt_str, *args):
    return FunctionWhat(What.Format, (fmt_str, ) + args)

class EdgeDirection(object):
    OUT = 0
    IN = 0
    BOTH = 0

def astar(src, dst, weight_field, options=None):
    return FunctionWhat(What.AStar, (src, dst, weight_field, options))

def dijkstra(src, dst, weight_field, direction=EdgeDirection.OUT):
    return FunctionWhat(What.Dijkstra, (src, dst, weight_field, direction))

def shortest_path(src, dst, direction=EdgeDirection.BOTH, edge_class=None, additional=None):
    return FunctionWhat(What.ShortestPath, (src, dst, direction, edge_class, additional))

def distance(x_field, y_field, x_value, y_value):
    return FunctionWhat(What.Distance, (x_field, y_field, x_value, y_value))

def distinct(field):
    return FunctionWhat(What.Distinct, (field,))

def unionall(field, *more):
    return FunctionWhat(What.UnionAll, (field, ) + more)

def intersect(field, *more):
    return FunctionWhat(What.Intersect, (field, ) + more)

def difference(field, *more):
    return FunctionWhat(What.Difference, (field, ) + more)

def symmetric_difference(field, *more):
    return FunctionWhat(What.SymmetricDifference, (field, ) + more)

def set(field):
    return FunctionWhat(What.Set, (field,))

def list(field):
    return FunctionWhat(What.List, (field,))

def map(key, value):
    return FunctionWhat(What.Map, (key, value))

def traversed_element(index, items=1):
    return FunctionWhat(What.TraversedElement, (index, items))

def traversed_edge(index, items=1):
    return FunctionWhat(What.TraversedEdge, (index, items))

def traversed_vertex(index, items=1):
    return FunctionWhat(What.TraversedVertex, (index, items))

def any():
    return FunctionWhat(What.Any, tuple())

def all():
    return FunctionWhat(What.All, tuple())

class ChainableWhat(What, Operand):
    def __init__(self, chain, props):
        super(ChainableWhat, self).__init__()
        self._chain = chain
        self._props = props

    def as_(self, name_override):
        if type(self) is not ChainableWhat:
            # Prevent further chaining
            self = ChainableWhat(self._chain, self._props)
        self.name_override = name_override

        return self

# Method mixins, according to type

class MethodWhatMixin(object):
    def asDecimal(self):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.AsDecimal,))

    def asFloat(self):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.AsFloat,))

    def asInteger(self):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.AsInteger,))

    def asList(self):
        return MethodWhat.prepare_next_link(self, CollectionMethodWhat, (What.AsList,))

    def asLong(self):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.AsLong,))

    def asSet(self):
        return MethodWhat.prepare_next_link(self, CollectionMethodWhat, (What.AsSet,))

    def asString(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.AsString,))

    def convert(self, to):
        # TODO Map to chainer type for type 'to'
        pass

    def format(self, format_str):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.FormatMethod, (format_str,)))

    def javaType(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.JavaType,))

    def type(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Type,))

class RecordMethodMixin(object):
    def toJSON(self, format_rules=None):
        # TODO Figure out the structure of format_rules
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.ToJSON,))

class StringMethodMixin(object):
    def charAt(self, position):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.CharAt, (position,)))

    def hash(self, hash_alg):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Hash, (hash_alg,)))

    def indexOf(self, needle, begin=0):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.IndexOf, (needle, begin)))

    def left(self, length):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Left, (length,)))

    def length(self):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.Length,tuple()))

    def normalize(self, form, pattern_matching):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Normalize, (form, pattern_matching)))

    def prefix(self, pre):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Prefix, (pre,)))

    def replace(self, old, new):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Replace, (old, new)))

    def right(self, length):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Right, (length,)))

    def subString(self, begin, length):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.SubString, (begin, length)))

    def trim(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Trim, tuple()))

    def toLowerCase(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.ToLowerCase, tuple()))

    def toUpperCase(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.ToUpperCase, tuple()))

class CollectionMethodMixin(object):
    def asMap(self):
        return MethodWhat.prepare_next_link(self, MapMethodWhat, (What.AsMap, tuple()))

    def remove(self, *items):
        # Disable further chaining
        return MethodWhat.prepare_next_link(self, ChainableWhat, (What.Remove, ) + items)

    def removeAll(self, *items):
        # Disable further chaining
        return MethodWhat.prepare_next_link(self, ChainableWhat, (What.RemoveAll, ) + items)

    def size(self):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.Size,))

class MapMethodMixin(CollectionMethodMixin):
    def keys(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Keys,))

    def values(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Values,))

class WhatFilterMixin(object):
    def __getitem__(self, filter_exp):
        self._chain.append((What.WhatFilter, filter_exp))
        return self

# Concrete method chaining types
class MethodWhat(MethodWhatMixin, ChainableWhat):
    def __init__(self, chain=[], props=[]):
        super(MethodWhat, self).__init__(chain, props)
        # Methods can also be chained to props
        self._method_chain = self._chain

    @staticmethod
    def prepare_next_link(current, chainer_type, link):
        current_type = type(current)
        if issubclass(current_type, ChainableWhat):
            if current_type is not chainer_type:
                # Constrain next link to type-compatible methods
                try:
                    current.__getattribute__('_immutable')
                    next_link = chainer_type(current._chain[:], current._props[:])
                except:
                    next_link = chainer_type(current._chain, current._props)
                next_link._method_chain.append(link)
                return next_link
            else:
                current._method_chain.append(link)
        else:
            return chainer_type([current, link], [])

        return current

class PropertyWhat(MethodWhatMixin, ChainableWhat):
    def __init__(self, chain, props):
        super(PropertyWhat, self).__init__(chain, props)

    def __getattr__(self, attr):
        self._props.append(attr)
        # Subsequent methods acting on props, not on chain
        self._method_chain = self._props
        return self

# Can't make assumptions about type of property
# Provide all method mixins, and assume user knows what they're doing
class AnyPropertyWhat(StringMethodMixin, MapMethodMixin, ArithmeticMixin, PropertyWhat):
    def __getitem__(self, item):
        self._props.append((What.WhatFilter, item))
        return self

class AnyPropertyMixin(object):
    def __getattr__(self, attr):
        # Prevent further chaining or use as record
        self = AnyPropertyWhat(self._chain, self._props)
        return self.__getattr__(attr)

class ElementWhat(RecordMethodMixin, CollectionMethodMixin, WhatFilterMixin, MethodWhat, AnyPropertyMixin):
    def at_rid(self):
        return MethodWhat.prepare_next_link(self, AtRid, (What.AtRid,))

    def at_class(self):
        return MethodWhat.prepare_next_link(self, AtClass, (What.AtClass,))

    def __call__(self):
        raise TypeError(
            '{} is not callable here.'.format(
                repr(self._props[-1]) if self._props else 'Query function'))

class VertexWhatMixin(object):
    def out(self, *labels):
        self._chain.append((What.Out, labels))
        return self

    def in_(self, *labels):
        self._chain.append((What.In, labels))
        return self

    def both(self, *labels):
        self._chain.append((What.Both, labels))
        return self

    def outE(self, *labels):
        chain = self._chain
        chain.append((What.OutE, labels))
        return EdgeWhat(chain)

    def inE(self, *labels):
        chain = self._chain
        chain.append((What.InE, labels))
        return EdgeWhat(chain)

    def bothE(self, *labels):
        chain = self._chain
        chain.append((What.BothE, labels))
        return EdgeWhat(chain)

class VertexWhat(VertexWhatMixin, ElementWhat):
    def __init__(self, chain):
        super(VertexWhat, self).__init__(chain, [])


class VertexWhatBegin(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, *labels):
        return VertexWhat([(self.func, labels)])

out = VertexWhatBegin(What.Out)
in_ = VertexWhatBegin(What.In)
both = VertexWhatBegin(What.Both)
outV = VertexWhatBegin(What.OutV)
inV = VertexWhatBegin(What.InV)

class EdgeWhatMixin(object):
    def outV(self):
        chain = self._chain
        chain.append((What.OutV,))
        return VertexWhat(chain)

    def inV(self):
        chain = self._chain
        chain.append((What.InV,))
        return VertexWhat(chain)

class EdgeWhat(EdgeWhatMixin, ElementWhat):
    def __init__(self, chain):
        super(EdgeWhat, self).__init__(chain, [])


class EdgeWhatBegin(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, *labels):
        return EdgeWhat([(self.func, labels)])

outE = EdgeWhatBegin(What.OutE)
inE = EdgeWhatBegin(What.InE)
bothE = EdgeWhatBegin(What.BothE)

class StringMethodWhat(StringMethodMixin, MethodWhat):
    pass

class CollectionMethodWhat(CollectionMethodMixin, MethodWhat):
    pass

class MapMethodWhat(MapMethodMixin, MethodWhat):
    pass

class LetVariable(ElementWhat):
    def __init__(self, name):
        super(LetVariable, self).__init__([(What.WhatLet, (name,))], [])

    def QV(self, name):
        self._chain.append((What.WhatLet, (name,)))
        return self

    def query(self):
        from .query import Query
        return Query(None, (self, ))

    def traverse(self, *what, **kwargs):
        from .traverse import Traverse
        return Traverse(None, self, *what, **kwargs)

class QV(LetVariable, VertexWhatMixin, EdgeWhatMixin, StringMethodMixin, MapMethodMixin, ArithmeticMixin):
    """Query Variable, used in graph SELECTs and TRAVERSEs.
       Specifies a number of classmethods for predefined variables; can also be
       created in LET clauses.
    """
    def __init__(self, name):
        """Reference a query variable
        :param name: Referenced variable (without leading '$')
        """
        if '.' in name:
            split = name.split('.')
            raise ValueError('Use QV({!r}).{} instead of QV({!r})'.format(
                split[0],
                '.'.join(['QV({!r})'.format(s[1:]) if s[0] == '$' else s for s in split[1:]]),
                name))
        super(QV, self).__init__(name)

    @classmethod
    def parent(cls):
        """Parent context from a sub-query (SELECT and TRAVERSE)"""
        return cls('parent')

    @classmethod
    def current(cls):
        """Current record in context of use (SELECT and TRAVERSE)"""
        return cls('current')

    @classmethod
    def parent_current(cls):
        """Shorthand for current record in parent's context (SELECT and TRAVERSE)"""
        return cls('parent').QV('current')

    @classmethod
    def root(cls):
        """Root context from a sub-query (SELECT and TRAVERSE)"""
        return cls('root')

    @classmethod
    def root_current(cls):
        """Shorthand for current record in root context (SELECT and TRAVERSE)"""
        return cls('root').QV('current')

    @classmethod
    def depth(cls):
        """The current depth of nesting (TRAVERSE)"""
        return cls('depth')

    @classmethod
    def path(cls):
        """String representation of the current (TRAVERSE) path"""
        return cls('path')

    @classmethod
    def stack(cls):
        """List of operations. Use to access the (TRAVERSE) history."""
        return cls('stack')

    @classmethod
    def history(cls):
        """All records traversed, as a Set<ORID> (TRAVERSE)"""
        return cls('history')

class QT(What):
    """Query token; for substitutions by RetrievalCommand.format() and its
    overrides
    :param ref: A token name, for matching keyword arguments
    """
    def __init__(self, ref=None):
        self.token = ref

    def query(self):
        """Query against a yet-unspecified source"""
        from .query import Query
        return Query(None, (self, ))

    def traverse(self, *what, **kwargs):
        """Traverse from a yet-unspecified target"""
        from .traverse import Traverse
        return Traverse(None, self, *what, **kwargs)

class QS(str):
    """Query string. Unquoted string substitutes to query tokens"""
    pass

class FunctionWhat(MethodWhat):
    """Derived from MethodWhat for the chain of which they might be the
    beginning
    """
    def __init__(self, func, args):
        super(FunctionWhat, self).__init__([(func, args)], [])

class CustomFunction(MethodWhat):
    """Call custom server-side functions from queries."""
    def __init__(self, name, expected, *args):
        super(CustomFunction, self).__init__([(What.WhatCustom, name, expected, args)], [])

def custom_function_handle(name, expected=(ArgConverter.Value,)):
    return lambda *args: CustomFunction(name, expected, *args)
# Record attributes

class RecordAttribute(object):
    """Base class for attributes which may be predefined for given records"""
    @classmethod
    def create_immutable(cls):
        """Mark created record attribute instance as read-only"""
        attribute = cls()
        setattr(attribute, '_immutable', True)
        return attribute

class AtThis(RecordAttribute, InstanceOfMixin, RecordMethodMixin, StringMethodWhat):
    """Denotes the record itself"""
    def __init__(self, chain=[(What.AtThis, tuple())], props=[]):
        super(AtThis, self).__init__(chain, props)

class AtRid(RecordAttribute, StringMethodWhat):
    """The record-id. null for embedded queries"""
    def __init__(self, chain=[(What.AtRid, tuple())], props=[]):
        super(AtRid, self).__init__(chain, props)

class AtClass(RecordAttribute, InstanceOfMixin, RecordMethodMixin, StringMethodWhat):
    """Class name, for schema-aware types"""
    def __init__(self, chain=[(What.AtClass, tuple())], props=[]):
        super(AtClass, self).__init__(chain, props)

class AtVersion(RecordAttribute, MethodWhat):
    """Integer record version; starts from zero"""
    def __init__(self, chain=[(What.AtVersion, tuple())], props=[]):
        super(AtVersion, self).__init__(chain, props)

class AtSize(RecordAttribute, MethodWhat):
    """The number of fields in the document"""
    def __init__(self, chain=[(What.AtSize, tuple())], props=[]):
        super(AtSize, self).__init__(chain, props)

class AtType(RecordAttribute, StringMethodWhat):
    """The record type"""
    def __init__(self, chain=[(What.AtType, tuple())], props=[]):
        super(AtType, self).__init__(chain, props)

at_this = AtThis.create_immutable()
at_class = AtClass.create_immutable()
at_rid = AtRid.create_immutable()
at_version = AtVersion.create_immutable()
at_size = AtSize.create_immutable()
at_type = AtType.create_immutable()

