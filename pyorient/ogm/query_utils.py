import pyorient.ogm.property

from .element import GraphElement

import pyorient.ogm.what
from .operators import LogicalConnective, ArithmeticOperation

class ArgConverter(object):
    """Convert query function argument to expected string format"""
    Label = String = Format = 0
    Expression = 1
    Field = 2
    Vertex = 3
    Value = 4
    Boolean = 5
    Name = 6
    Filter = 7

    @staticmethod
    def convert_to(conversion, arg, for_query):
        if conversion is ArgConverter.Label:
            return '{}'.format(pyorient.ogm.property.PropertyEncoder.encode_value(arg, for_query))
        elif conversion is ArgConverter.Expression:
            if isinstance(arg, LogicalConnective):
                return '\'{}\''.format(for_query.filter_string(arg))
            elif isinstance(arg, ArithmeticOperation):
                return '\'{}\''.format(for_query.arithmetic_string(arg))
            else:
                return repr(arg)
        elif conversion is ArgConverter.Field:
            if isinstance(arg, pyorient.ogm.property.Property):
                return arg.context_name()
            elif isinstance(arg, GraphElement):
                return arg.registry_name
            elif isinstance(arg, pyorient.ogm.what.What):
                return for_query.build_what(arg)
            else:
                return arg
        elif conversion is ArgConverter.Vertex:
            if isinstance(arg, GraphElement):
                return arg._id
            elif isinstance(arg, pyorient.ogm.what.What):
                return for_query.build_what(arg)
            else:
                return pyorient.ogm.property.PropertyEncoder.encode_value(arg, for_query)
        elif conversion is ArgConverter.Value:
            from pyorient.ogm.commands import RetrievalCommand
            if isinstance(arg, pyorient.ogm.property.Property):
                return arg.context_name()
            elif isinstance(arg, GraphElement):
                return arg.registry_name
            elif isinstance(arg, pyorient.ogm.what.What):
                return for_query.build_what(arg)
            elif isinstance(arg, ArithmeticOperation):
                return for_query.arithmetic_string(arg)
            elif isinstance(arg, RetrievalCommand):
                # e.g., in What.if_()
                return '(' + str(arg) + ')'
            else:
                return pyorient.ogm.property.PropertyEncoder.encode_value(arg, for_query)
        elif conversion is ArgConverter.Boolean:
            if isinstance(arg, pyorient.ogm.what.What):
                return for_query.build_what(arg)
            elif isinstance(arg, LogicalConnective):
                return for_query.filter_string(arg)
            else:
                return 'true' if arg else 'false'
        elif conversion is ArgConverter.Name:
            return pyorient.ogm.property.PropertyEncoder.encode_name(arg)
        elif conversion is ArgConverter.Filter:
            if isinstance(arg, LogicalConnective):
                return for_query.filter_string(arg)
            else:
                return repr(arg)
        else:
            pass


