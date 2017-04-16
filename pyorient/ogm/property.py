from .operators import Operand, ArithmeticMixin
from .what import (
    What, FunctionWhat
    , StringMethodMixin
    , CollectionMethodMixin
    , MapMethodMixin
    , PropertyWhat
)
from .element import GraphElement

import json
import datetime
import decimal
import string
import sys


class Property(PropertyWhat):
    num_instances = 0 # Basis for ordering property instances

    def __init__(self, name=None, nullable=True
                 , default=None, indexed=False, unique=False
                 , mandatory=False, readonly=False):
        """Create a database class property.

        :param name: Overrides name of class attribute used for property
        instance
        :param nullable: True if property may be null/None, False otherwise
        :param default: Property's default value
        :param indexed: True if index to be created for property, False
        otherwise
        :param unique: Uniqueness of property value enforced when True; create
        index
        :param mandatory: Value must be provided for property. Property will
        automatically become mandatory if not nullable.
        :param readonly: Property value can not be changed after first
        assignment.
        """
        super(Property, self).__init__([], [])

        self.name = name

        if nullable:
            self.nullable = True
            self.mandatory = mandatory
        else:
            self.nullable = False
            self.mandatory = True

        self.default = default
        self.indexed = indexed or unique
        self.unique = unique
        self.readonly = readonly

        self._context = None

        # Class creation shouldn't straddle multiple threads...
        self.instance_idx = Property.num_instances
        Property.num_instances += 1

    @property
    def context(self):
        """Get containing context."""
        return self._context

    @context.setter
    def context(self, context):
        """Set containing context.

        A property should not be shared between multiple contexts."""
        self._context = context

    def context_name(self):
        if self.name:
            return self.name
        for prop_name, prop_value in self.context.__dict__.items():
            if self is prop_value:
                return prop_name
        else:
            raise NameError('Somehow this property\'s context is broken.')

    def __format__(self, format_spec):
        return repr(self.context_name())

class UUID:
    def __str__(self):
        return 'UUID()'

class PropertyEncoder:
    PROHIBITED_NAME_CHARS = set(''.join([string.whitespace, '"\'']))

    @staticmethod
    def encode_name(name):
        for c in name:
            if c in PropertyEncoder.PROHIBITED_NAME_CHARS:
                raise ValueError('Prohibited character in property name: {}'.format(name))
        return name

    @staticmethod
    def encode_value(value):
        if isinstance(value, decimal.Decimal):
            return u'"{:f}"'.format(value)
        elif isinstance(value, float):
            with decimal.localcontext() as ctx:
                ctx.prec = 20  # floats are max 80-bits wide = 20 significant digits
                return u'"{:f}"'.format(decimal.Decimal(value))
        elif isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
            return u'"{}"'.format(value)
        elif isinstance(value, str):
            # it just so happens that JSON in ASCII mode has the same limitations
            # and escape sequences as what we need: \u00c5 vs \xc5 representation,
            # quote escaping etc.
            return json.dumps(value)
        elif sys.version_info[0] < 3 and isinstance(value, unicode):
            return json.dumps(value)
        elif value is None:
            return 'null'
        elif isinstance(value, (int,float)) or (sys.version_info[0] < 3 and isinstance(value, long)):
            return str(value)
        elif isinstance(value, list) or isinstance(value, set):
            return u'[{}]'.format(u','.join([PropertyEncoder.encode_value(v) for v in value]))
        elif isinstance(value, dict):
            contents = u','.join([
                '{}: {}'.format(PropertyEncoder.encode_value(k), PropertyEncoder.encode_value(v))
                for k, v in value.items()
            ])
            return u'{{ {} }}'.format(contents)
        elif isinstance(value, FunctionWhat) and value.chain[0][0] == What.SysDate:
            return 'sysdate({})'.format(','.join([PropertyEncoder.encode_value(v) for v in value.chain[0][1] if v is not None]))
        elif isinstance(value, GraphElement):
            return value._id
        else:
            # returning the same object will cause repr(value) to be used
            return value

class Boolean(Property):
    pass

class Integer(Property, ArithmeticMixin):
    pass

class Short(Property, ArithmeticMixin):
    pass

class Long(Property, ArithmeticMixin):
    pass

class Float(Property, ArithmeticMixin):
    pass

class Double(Property, ArithmeticMixin):
    pass

class DateTime(Property):
    pass

class String(Property, StringMethodMixin):
    pass

class Binary(Property):
    pass

class Byte(Property):
    pass

class Date(Property):
    pass

class Decimal(Property, ArithmeticMixin):
    pass

class Embedded(Property):
    pass

class LinkedClassProperty(Property):
    def __init__(self, linked_to=None, name=None, default=None,
                 nullable=True, unique=False, indexed=False,
                 mandatory=False, readonly=False):
        """Create a property representing a collection of entries or a link.

        :param linked_to: Entry type; optional, as per 'CREATE PROPERTY' syntax
        """
        super(LinkedClassProperty, self).__init__(
            name, nullable, default, indexed, unique, mandatory, readonly)
        self.linked_to = linked_to

class Link(LinkedClassProperty):
    pass

class LinkList(LinkedClassProperty, CollectionMethodMixin):
    pass

class LinkSet(LinkedClassProperty, CollectionMethodMixin):
    pass

class LinkMap(LinkedClassProperty, MapMethodMixin):
    pass

class LinkedProperty(LinkedClassProperty):
    """A LinkedProperty, unlike a LinkedClassProperty, can also link to
    primitive types"""
    pass

class EmbeddedList(LinkedProperty, CollectionMethodMixin):
    pass

class EmbeddedSet(LinkedProperty, CollectionMethodMixin):
    pass

class EmbeddedMap(LinkedProperty, MapMethodMixin):
    pass
