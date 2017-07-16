from .operators import (Operator, IdentityOperand, Operand,
                        ArithmeticOperation, LogicalConnective)
from pyorient.ogm.what import What, FunctionWhat, ChainableWhat, LetVariable, QT
from pyorient.ogm.property import Property, PropertyEncoder
from pyorient.ogm.query_utils import ArgConverter

from collections import namedtuple

import json

import sys
if sys.version < '3':
    from itertools import izip_longest as zip_longest
else:
    from itertools import zip_longest

class ExpressionMixin(object):
    WhatFunction = namedtuple('what', ['max_args', 'fmt', 'expected'])
    WhatFunctions = {
        # TODO handle GraphElement args
        What.Out: WhatFunction(1, 'out({})', (ArgConverter.Label,))
        , What.In: WhatFunction(1, 'in({})', (ArgConverter.Label,))
        , What.Both: WhatFunction(1, 'both({})', (ArgConverter.Label,))
        , What.OutE: WhatFunction(1, 'outE({})', (ArgConverter.Label,))
        , What.InE: WhatFunction(1, 'inE({})', (ArgConverter.Label,))
        , What.BothE: WhatFunction(1, 'bothE({})', (ArgConverter.Label,))
        , What.OutV: WhatFunction(0, 'outV()', tuple())
        , What.InV: WhatFunction(0, 'inV()', tuple())
        , What.Eval: WhatFunction(1, 'eval({})', (ArgConverter.Expression,))
        , What.Coalesce: WhatFunction(None, 'coalesce({})'
                                      , (ArgConverter.Field,))
        , What.If: WhatFunction(3, 'if({})'
                                , (ArgConverter.Boolean, ArgConverter.Value
                                   , ArgConverter.Value))
        , What.IfNull: WhatFunction(2, 'ifnull({})'
                                    , (ArgConverter.Field, ArgConverter.Vertex))
        , What.Expand: WhatFunction(1, 'expand({})', (ArgConverter.Field,))
        , What.First: WhatFunction(1, 'first({})', (ArgConverter.Field,))
        , What.Last: WhatFunction(1, 'last({})', (ArgConverter.Field,))
        , What.Count: WhatFunction(1, 'count({})', (ArgConverter.Field,))
        , What.Min: WhatFunction(None, 'min({})', (ArgConverter.Field,))
        , What.Max: WhatFunction(None, 'max({})', (ArgConverter.Field,))
        , What.Abs: WhatFunction(1, 'abs({})', (ArgConverter.Field,))
        , What.Avg: WhatFunction(1, 'avg({})', (ArgConverter.Field,))
        , What.Mode: WhatFunction(1, 'mode({})', (ArgConverter.Field,))
        , What.Median: WhatFunction(1, 'median({})', (ArgConverter.Field,))
        , What.Percentile: WhatFunction(None, 'percentile({})'
                                        , (ArgConverter.Field,))
        , What.Variance: WhatFunction(1, 'variance({})', (ArgConverter.Field,))
        , What.StdDev: WhatFunction(1, 'stddev({})', (ArgConverter.Field,))
        , What.Sum: WhatFunction(1, 'sum({})', (ArgConverter.Field,))
        , What.Date: WhatFunction(3, 'date({})'
                                  , (ArgConverter.String, ArgConverter.String
                                     , ArgConverter.String))
        , What.SysDate: WhatFunction(2, 'sysdate({})'
                                     , (ArgConverter.String
                                        , ArgConverter.String))
        , What.Format: WhatFunction(None, 'format({})'
                                    , (ArgConverter.Format, ArgConverter.Field))
        , What.AStar:
            WhatFunction(4, 'astar({}'
                         , (ArgConverter.Vertex, ArgConverter.Vertex
                            , ArgConverter.Label, ArgConverter.Label))
        , What.Dijkstra:
            WhatFunction(4, 'dijkstra({})'
                         , (ArgConverter.Vertex, ArgConverter.Vertex
                         , ArgConverter.Label, ArgConverter.Value))
        , What.ShortestPath:
            WhatFunction(5, 'shortestPath({})'
                         , (ArgConverter.Vertex, ArgConverter.Vertex
                            , ArgConverter.Value, ArgConverter.Label
                            , ArgConverter.Label))
        , What.Distance:
            WhatFunction(4, 'distance({})'
                         , (ArgConverter.Field, ArgConverter.Field
                            , ArgConverter.Value, ArgConverter.Value))
        , What.Distinct: WhatFunction(1, 'distinct({})', (ArgConverter.Field,))
        , What.UnionAll: WhatFunction(None, 'unionall({})'
                                      , (ArgConverter.Field,))
        , What.Intersect: WhatFunction(None, 'intersect({})'
                                       , (ArgConverter.Field,))
        , What.Difference: WhatFunction(None, 'difference({})'
                                        , (ArgConverter.Field,))
        , What.SymmetricDifference:
            WhatFunction(None, 'symmetricDifference({})', (ArgConverter.Field,))
        # FIXME Don't understand usage of these, yet.
        , What.Set: WhatFunction(1, 'set({})', (ArgConverter.Field,))
        , What.List: WhatFunction(1, 'list({})', (ArgConverter.Field,))
        , What.Map: WhatFunction(2, 'map({})', (ArgConverter.Field
                                                , ArgConverter.Field))
        , What.TraversedElement:
            WhatFunction(2, 'traversedElement({})'
                         , (ArgConverter.Value, ArgConverter.Value))
        , What.TraversedEdge:
            WhatFunction(2, 'traversedEdge({})'
                         , (ArgConverter.Value, ArgConverter.Value))
        , What.TraversedVertex:
            WhatFunction(2, 'traversedVertex({})'
                         , (ArgConverter.Value, ArgConverter.Value))
        , What.Any: WhatFunction(0, 'any()', tuple())
        , What.All: WhatFunction(0, 'all()', tuple())
        # Methods
        , What.Append: WhatFunction(1, 'append({})', (ArgConverter.Value,))
        , What.AsBoolean: WhatFunction(0, 'asBoolean()', tuple())
        , What.AsDate: WhatFunction(0, 'asDate()', tuple())
        , What.AsDatetime: WhatFunction(0, 'asDatetime()', tuple())
        , What.AsDecimal: WhatFunction(0, 'asDecimal()', tuple())
        , What.AsFloat: WhatFunction(0, 'asFloat()', tuple())
        , What.AsInteger: WhatFunction(0, 'asInteger()', tuple())
        , What.AsList: WhatFunction(0, 'asList()', tuple())
        , What.AsLong: WhatFunction(0, 'asLong()', tuple())
        , What.AsMap: WhatFunction(0, 'asMap()', tuple())
        , What.AsSet: WhatFunction(0, 'asSet()', tuple())
        , What.AsString: WhatFunction(0, 'asString()', tuple())
        , What.CharAt: WhatFunction(1, 'charAt({})', (ArgConverter.Field,))
        , What.Convert: WhatFunction(1, 'convert({})', (ArgConverter.Value,))
        , What.Exclude: WhatFunction(None, 'exclude({})', (ArgConverter.Value,))
        , What.FormatMethod: WhatFunction(1, 'format({})', (ArgConverter.Value,))
        , What.Hash: WhatFunction(1, 'hash({})', (ArgConverter.Value,))
        , What.Include: WhatFunction(None, 'include({})', (ArgConverter.Value,))
        , What.IndexOf: WhatFunction(2, 'indexOf({})', (ArgConverter.Value, ArgConverter.Value))
        , What.JavaType: WhatFunction(0, 'javaType()', tuple())
        , What.Keys: WhatFunction(0, 'keys()', tuple())
        , What.Left: WhatFunction(1, 'left({})', (ArgConverter.Value,))
        , What.Length: WhatFunction(0, 'length()', tuple())
        , What.Normalize: WhatFunction(2, 'normalize({})', (ArgConverter.Value, ArgConverter.Value))
        , What.Prefix: WhatFunction(1, 'prefix({})', (ArgConverter.Value,))
        , What.Remove: WhatFunction(None, 'remove({})', (ArgConverter.Value,))
        , What.RemoveAll: WhatFunction(None, 'removeAll({})', (ArgConverter.Value,))
        , What.Replace: WhatFunction(2, 'replace({})', (ArgConverter.Value, ArgConverter.Value))
        , What.Right: WhatFunction(1, 'right({})', (ArgConverter.Value,))
        , What.Size: WhatFunction(0, 'size()', tuple())
        , What.SubString: WhatFunction(2, 'substring({})', (ArgConverter.Value, ArgConverter.Value))
        , What.Trim: WhatFunction(0, 'trim()', tuple())
        , What.ToJSON: WhatFunction(0, 'toJSON()', tuple()) # FIXME TODO Figure out format argument
        , What.ToLowerCase: WhatFunction(0, 'toLowerCase()', tuple())
        , What.ToUpperCase: WhatFunction(0, 'toUpperCase()', tuple())
        , What.Type: WhatFunction(0, 'type()', tuple())
        , What.Values: WhatFunction(0, 'values()', tuple())
        , What.WhatLet: WhatFunction(1, '${}', (ArgConverter.Name, ))
        , What.VertexClass: WhatFunction(0, 'V', tuple())
        , What.EdgeClass: WhatFunction(0, 'E', tuple())
        , What.EdgeIn: WhatFunction(0, 'in', tuple())
        , What.EdgeOut: WhatFunction(0, 'out', tuple())
        , What.AtThis: WhatFunction(0, '@this', tuple())
        , What.AtRid: WhatFunction(0, '@rid', tuple())
        , What.AtClass: WhatFunction(0, '@class', tuple())
        , What.AtVersion: WhatFunction(0, '@version', tuple())
        , What.AtSize: WhatFunction(0, '@size', tuple())
        , What.AtType: WhatFunction(0, '@type', tuple())
        , What.Sequence: WhatFunction(1, 'sequence({})', (ArgConverter.Label,))
        , What.Current: WhatFunction(0, 'current()', tuple())
        , What.Next: WhatFunction(0, 'next()', tuple())
    }

    @classmethod
    def filter_string(cls, expression_root):
        op = expression_root.operator

        left = expression_root.operands[0]
        right = expression_root.operands[1]
        if isinstance(left, IdentityOperand):
            if isinstance(left, Property):
                left_str = left.context_name()
            elif isinstance(left, ArithmeticOperation):
                left_str = u'({})'.format(cls.arithmetic_string(left))
            elif isinstance(left, ChainableWhat):
                left_str = cls.build_what(left)
            else:
                raise ValueError(
                    'Operator {} not supported as a filter'.format(op))

            if op is Operator.Equal:
                return u'{0} = {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Vertex
                                                      , right, cls))
            elif op is Operator.GreaterEqual:
                return u'{0} >= {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, cls))
            elif op is Operator.Greater:
                return u'{0} > {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, cls))
            elif op is Operator.LessEqual:
                return u'{0} <= {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, cls))
            elif op is Operator.Less:
                return u'{0} < {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Value
                                                      , right, cls))
            elif op is Operator.NotEqual:
                return u'{0} <> {1}'.format(
                    left_str, ArgConverter.convert_to(ArgConverter.Vertex
                                                      , right, cls))
            elif op is Operator.Between:
                far_right = PropertyEncoder.encode_value(expression_root.operands[2], cls)
                return u'{0} BETWEEN {1} and {2}'.format(
                    left_str, PropertyEncoder.encode_value(right, cls), far_right)
            elif op is Operator.Contains:
                if isinstance(right, LogicalConnective):
                    return u'{0} contains({1})'.format(
                        left_str, cls.filter_string(right))
                else:
                    return u'{} in {}'.format(
                        PropertyEncoder.encode_value(right, cls), left_str)
            elif op is Operator.EndsWith:
                return u'{0} like {1}'.format(left_str, PropertyEncoder.encode_value('%' + right, cls))
            elif op is Operator.Is:
                if not right: # :)
                    return '{0} is null'.format(left_str)
            elif op is Operator.IsNot:
                if not right:
                    return '{} is not null'.format(left_str)
            elif op is Operator.Like:
                return u'{0} like {1}'.format(
                    left_str, PropertyEncoder.encode_value(right, cls))
            elif op is Operator.Matches:
                return u'{0} matches {1}'.format(
                    left_str, PropertyEncoder.encode_value(right, cls))
            elif op is Operator.StartsWith:
                return u'{0} like {1}'.format(
                    left_str, PropertyEncoder.encode_value(right + '%', cls))
            elif op is Operator.InstanceOf:
                return u'{0} instanceof {1}'.format(
                    left_str, repr(right.registry_name))
            else:
                raise AssertionError('Unhandled Operator type: {}'.format(op))
        else:
            return u'{0} {1} {2}'.format(
                cls.filter_string(left)
                , 'and' if op is Operator.And else 'or'
                , cls.filter_string(right))

    @classmethod
    def arithmetic_string(cls, operation_root):
        if isinstance(operation_root, ArithmeticOperation):
            op = operation_root.operator
            if operation_root.paren:
                lp = '('
                rp = ')'
            else:
                lp = rp = ''

            left = operation_root.operands[0]
            # Unary operators not yet supported?
            right = operation_root.operands[1]

            if op is Operator.Add:
                exp = '{} + {}'.format(
                        cls.arithmetic_string(left)
                        , cls.arithmetic_string(right))
            elif op is Operator.Sub:
                exp = '{} - {}'.format(
                        cls.arithmetic_string(left)
                        , cls.arithmetic_string(right))
            elif op is Operator.Mul:
                exp = '{} * {}'.format(
                        cls.arithmetic_string(left)
                        , cls.arithmetic_string(right))
            elif op is Operator.Div:
                exp = '{} / {}'.format(
                        cls.arithmetic_string(left)
                        , cls.arithmetic_string(right))
            elif op is Operator.Mod:
                exp = '{} % {}'.format(
                        cls.arithmetic_string(left)
                        , cls.arithmetic_string(right))

            return lp+exp+rp
        elif isinstance(operation_root, Property):
            return operation_root.context_name()
        elif isinstance(operation_root, LetVariable) or isinstance(operation_root, QT):
            # TODO This condition suggests common base for variables and tokens, below What
            return cls.build_what(operation_root)
        else:
            return operation_root

    @classmethod
    def build_what(cls, what, prop_names=None):
        if isinstance(what, Property):
            prop_name = what.context_name()
            if prop_names is not None:
                prop_names.append(prop_name)
            return prop_name
        elif not isinstance(what, What):
            if isinstance(what, str):
                what_str = json.dumps(what)
            else:
                what_str = str(what)

            if prop_names is not None:
                period = what_str.find('.')
                if period >= 0:
                    prop_names.append(what_str[0:period])
                else:
                    prop_names.append(what_str.replace('"', ''))
            return what_str

        if isinstance(what, FunctionWhat):
            func = what.chain[0][0]
            what_function = cls.WhatFunctions[func]

            name_override = what.name_override
            as_str = ' AS {}'.format(name_override) if name_override else ''
            if prop_names is not None:
                # Projections not allowed with Expand
                counted = func is not What.Expand
                if counted:
                    prop_names.append(
                        cls.parse_prop_name(what_function.fmt, name_override))

            return '{}{}'.format(
                what_function.fmt.format(
                    ','.join(cls.what_args(what_function.expected,
                                            what.chain[0][1]))), as_str)
        elif isinstance(what, ChainableWhat):
            chain = []
            for func_args in what.chain:
                func_key = func_args[0]
                if func_key == What.WhatFilter:
                    filter_exp = func_args[1]
                    chain[-1] += '[{}]'.format(ArgConverter.convert_to(ArgConverter.Filter, filter_exp, cls))
                    continue
                elif func_key == What.WhatCustom:
                    chain.append('{}({})'.format(func_args[1], ','.join(cls.what_args(func_args[2], func_args[3]))))
                    continue

                cls.append_what_function(chain, func_key, func_args)

            for prop in what.props:
                if isinstance(prop, tuple):
                    func_key = prop[0]
                    if func_key == What.WhatFilter:
                        chain[-1] += '[{}]'.format(prop[1])
                    else:
                        cls.append_what_function(chain, func_key, prop)
                else:
                    chain.append(prop)

            name_override = what.name_override
            as_str = ' AS {}'.format(name_override) if name_override else ''
            if prop_names is not None:
                prop_names.append(
                    cls.parse_prop_name(chain[0], name_override))
            return '.'.join(chain) + as_str
        else:
            # For now, can assume it's a Token
            return '{{{}}}'.format(what.token) if what.token is not None else '{}'

    @staticmethod
    def parse_prop_name(from_str, override):
        if override:
            return override
        else:
            paren_idx = from_str.find('(')
            if paren_idx < 0:
                return from_str
            else:
                return from_str[:paren_idx]

    @classmethod
    def what_args(cls, expected, args):
        if args:
            return [ArgConverter.convert_to(conversion, arg, cls)
                    for arg, conversion in
                        zip_longest(args, expected
                                    , fillvalue=expected[-1])
                        if arg is not None]
        else:
            return []

    @classmethod
    def append_what_function(cls, chain, func_key, func_args):
        what_function = cls.WhatFunctions[func_key]
        max_args = what_function.max_args
        if max_args > 0 or max_args is None:
            chain.append(
                what_function.fmt.format(
                    ','.join(cls.what_args(what_function.expected,
                                            func_args[1]))))
        else:
            chain.append(what_function.fmt)
