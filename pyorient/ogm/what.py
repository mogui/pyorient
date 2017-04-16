from pyorient.ogm.operators import (IdentityOperand, RelativeOperand, Operand, InstanceOfMixin)
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
    Avg = 18
    Mode = 19
    Median = 20
    Percentile = 21
    Variance = 22
    StdDev = 23
    Sum = 24
    Date = 25
    SysDate = 26
    Format = 27
    Dijkstra = 28
    ShortestPath = 29
    Distance = 30
    Distinct = 31
    UnionAll = 32
    Intersect = 33
    Difference = 34
    SymmetricDifference = 35
    Set = 36
    List = 37
    Map = 38
    TraversedElement = 39
    TraversedEdge = 40
    TraversedVertex = 41
    Any = 42
    All = 43
    # Methods
    Subscript = 44
    Append = 45
    AsBoolean = 46
    AsDate = 47
    AsDatetime = 48
    AsDecimal = 49
    AsFloat = 50
    AsInteger = 51
    AsList = 52
    AsLong = 53
    AsMap = 54
    AsSet = 55
    AsString = 56
    CharAt = 57
    Convert = 58
    Exclude = 59
    FormatMethod = 60
    Hash = 61
    Include = 62
    IndexOf = 63
    JavaType = 64
    Keys = 65
    Left = 66
    Length = 67
    Normalize = 68
    Prefix = 69
    Remove = 70
    RemoveAll = 71
    Replace = 72
    Right = 73
    Size = 74
    SubString = 75
    Trim = 76
    ToJSON = 77
    ToLowerCase = 78
    ToUpperCase = 79
    Type = 80
    Values = 81
    # Filter
    WhatFilter = 82
    # Custom functions
    WhatCustom = 83
    # Let
    WhatLet = 84
    # Record attributes
    AtThis = 85
    AtRid = 86
    AtClass = 87
    AtVersion = 88
    AtSize = 89
    AtType = 90

    def __init__(self):
        self.name_override = None

    def as_(self, name_override):
        self.name_override = name_override
        return self

def eval_(exp):
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

def avg(field):
    return FunctionWhat(What.Avg, (field,))

def mode(field):
    return FunctionWhat(What.Mode, (field,))

def median(field):
    return FunctionWhat(What.Median, (field,))

def percentile(field, *quantiles):
    return FunctionWhat(What.Percentile, (field, quantiles))

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
    return FunctionWhat(What.Format, (fmt_str, args))

class EdgeDirection(object):
    OUT = 0
    IN = 0
    BOTH = 0

def dijkstra(src, dst, weight_field, direction=EdgeDirection.OUT):
    return FunctionWhat(What.Dijkstra, (src, dst, weight_field, direction))

def shortest_path(src, dst, direction=EdgeDirection.BOTH, edge_class=None):
    return FunctionWhat(What.ShortestPath, (src, dst, direction, edge_class))

def distance(x_field, y_field, x_value, y_value):
    return FunctionWhat(What.Distance, (x_field, y_field, x_value, y_value))

def distinct(field):
    return FunctionWhat(What.Distinct, (field,))

def unionall(field, *more):
    return FunctionWhat(What.UnionAll, (field, more))

def intersect(field, *more):
    return FunctionWhat(What.Intersect, (field, more))

def difference(field, *more):
    return FunctionWhat(What.Difference, (field, more))

def symmetric_difference(field, *more):
    return FunctionWhat(What.SymmetricDifference, (field, more))

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

class ChainableWhat(What):
    def __init__(self, chain, props):
        super(ChainableWhat, self).__init__()
        self.chain = chain
        self.props = props

    def as_(self, name_override):
        if type(self) is not ChainableWhat:
            # Prevent further chaining
            self = ChainableWhat(self.chain, self.props)
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
        return MethodWhat.prepare_next_link(self, ChainableWhat, (What.Remove, items))

    def removeAll(self, *items):
        # Disable further chaining
        return MethodWhat.prepare_next_link(self, ChainableWhat, (What.RemoveAll, items))

    def size(self):
        return MethodWhat.prepare_next_link(self, MethodWhat, (What.Size,))

class MapMethodMixin(CollectionMethodMixin):
    def keys(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Keys,))

    def values(self):
        return MethodWhat.prepare_next_link(self, StringMethodWhat, (What.Values,))

class WhatFilterMixin(object):
    def __getitem__(self, filter_exp):
        self.chain.append((What.WhatFilter, filter_exp))
        return self

# Concrete method chaining types
class MethodWhat(MethodWhatMixin, Operand, ChainableWhat):
    def __init__(self, chain=[], props=[]):
        super(MethodWhat, self).__init__(chain, props)
        # Methods can also be chained to props
        self.method_chain = self.chain

    @staticmethod
    def prepare_next_link(current, chainer_type, link):
        current_type = type(current)
        if issubclass(current_type, ChainableWhat):
            if current_type is not chainer_type:
                # Constrain next link to type-compatible methods
                try:
                    current.__getattribute__('_immutable')
                    next_link = chainer_type(current.chain[:], current.props[:])
                except:
                    next_link = chainer_type(current.chain, current.props)
                next_link.method_chain.append(link)
                return next_link
            else:
                current.method_chain.append(link)
        else:
            return chainer_type([current, link], [])

        return current

class ElementWhat(RecordMethodMixin, WhatFilterMixin, MethodWhat):
    def at_rid(self):
        return MethodWhat.prepare_next_link(self, AtRid, (What.AtRid,))

    def __getattr__(self, attr):
        # Prevent further chaining or use as record
        self = AnyPropertyWhat(self.chain, self.props)
        return self.__getattr__(attr)

    def __call__(self):
        raise TypeError(
            '{} is not callable here.'.format(
                repr(self.props[-1]) if self.props else 'Query function'))

class PropertyWhat(MethodWhatMixin, Operand, ChainableWhat):
    def __init__(self, chain, props):
        super(PropertyWhat, self).__init__(chain, props)

    def __getattr__(self, attr):
        self.props.append(attr)
        # Subsequent methods acting on props, not on chain
        self.method_chain = self.props
        return self

# Can't make assumptions about type of property
# Provide all method mixins, and assume user knows what they're doing
class AnyPropertyWhat(StringMethodMixin, MapMethodMixin, PropertyWhat):
    pass

class VertexWhatMixin(object):
    def out(self, *labels):
        self.chain.append((What.Out, labels))
        return self

    def in_(self, *labels):
        self.chain.append((What.In, labels))
        return self

    def both(self, *labels):
        self.chain.append((What.Both, labels))
        return self

    def outE(self, *labels):
        chain = self.chain
        chain.append((What.OutE, labels))
        return EdgeWhat(chain)

    def inE(self, *labels):
        chain = self.chain
        chain.append((What.InE, labels))
        return EdgeWhat(chain)

    def bothE(self, *labels):
        chain = self.chain
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
        chain = self.chain
        chain.append((What.OutV,))
        return VertexWhat(chain)

    def inV(self):
        chain = self.chain
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

class QV(VertexWhatMixin, EdgeWhatMixin, WhatFilterMixin, RecordMethodMixin, StringMethodMixin, MapMethodMixin, MethodWhat):
    def __init__(self, name):
        super(QV, self).__init__([(What.WhatLet, (name,))], [])

    def QV(self, name):
        self.chain.append((What.WhatLet, (name,)))
        return self

    @classmethod
    def parent(cls):
        return cls('parent')

    @classmethod
    def parent_current(cls):
        return cls('parent').QV('current')

class FunctionWhat(MethodWhat):
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
    @classmethod
    def create_immutable(cls):
        attribute = cls()
        setattr(attribute, '_immutable', True)
        return attribute

class AtThis(RecordAttribute, InstanceOfMixin, RecordMethodMixin, StringMethodWhat):
    def __init__(self, chain=[(What.AtThis, tuple())], props=[]):
        super(AtThis, self).__init__(chain, props)


class AtRid(RecordAttribute, StringMethodWhat):
    def __init__(self, chain=[(What.AtRid, tuple())], props=[]):
        super(AtRid, self).__init__(chain, props)

class AtClass(RecordAttribute, InstanceOfMixin, RecordMethodMixin, StringMethodWhat):
    def __init__(self, chain=[(What.AtClass, tuple())], props=[]):
        super(AtClass, self).__init__(chain, props)

class AtVersion(RecordAttribute, MethodWhat):
    def __init__(self, chain=[(What.AtVersion, tuple())], props=[]):
        super(AtVersion, self).__init__(chain, props)

class AtSize(RecordAttribute, MethodWhat):
    def __init__(self, chain=[(What.AtSize, tuple())], props=[]):
        super(AtSize, self).__init__(chain, props)

class AtType(RecordAttribute, StringMethodWhat):
    def __init__(self, chain=[(What.AtType, tuple())], props=[]):
        super(AtType, self).__init__(chain, props)

at_this = AtThis.create_immutable()
at_class = AtClass.create_immutable()
at_rid = AtRid.create_immutable()
at_version = AtVersion.create_immutable()
at_size = AtSize.create_immutable()
at_type = AtType.create_immutable()
