from .operators import RelativeOperand

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

class FunctionWhat(What, RelativeOperand):
    def __init__(self, func, args):
        self.func = func
        self.args = args
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
    return FunctionWhat(What.Any, None)

def all():
    return FunctionWhat(What.All, None)

class ChainableWhat(What):
    def __init__(self, chain, props):
        self.chain = chain
        self.props = props
        self.name_override = None

    def as_(self, name_override):
        if type(self) is not ChainableWhat:
            # Prevent further chaining
            self = ChainableWhat(self.chain, self.props)
        self.name_override = name_override

        return self

class ElementWhat(ChainableWhat):
    def __getattr__(self, attr):
        if type(self) is not ElementWhat:
            # Prevent further chaining
            self = ElementWhat(self.chain, self.props)
        self.props.append(attr)

        return self

    def __call__(self):
        raise TypeError(
            '{} is not callable here.'.format(
                repr(self.props[-1]) if self.props else 'Query function'))


class VertexWhat(ElementWhat):
    def __init__(self, chain):
        super(VertexWhat, self).__init__(chain, [])

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

class EdgeWhat(ElementWhat):
    def __init__(self, chain):
        super(EdgeWhat, self).__init__(chain, [])

    def outV(self):
        chain = self.chain
        chain.append((What.OutV,))
        return VertexWhat(chain)

    def inV(self):
        chain = self.chain
        chain.append((What.InV,))
        return VertexWhat(chain)

class EdgeWhatBegin(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, *labels):
        return EdgeWhat([(self.func, labels)])

outE = EdgeWhatBegin(What.OutE)
inE = EdgeWhatBegin(What.InE)
bothE = EdgeWhatBegin(What.BothE)

