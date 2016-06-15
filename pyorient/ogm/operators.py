#from enum import Enum

# Enum only in Python >= 3.4
#class Operator(Enum):
class Operator(object):
    And = 0
    Equal = 1
    GreaterEqual = 2
    Greater = 3
    LessEqual = 4
    Less = 5
    NotEqual = 6
    Or = 7
    Between = 8
    Contains = 9
    EndsWith = 10
    Is = 11
    IsNot = 12
    Like = 13
    Matches = 14
    StartsWith = 15
    Add = 16
    Sub = 17
    Mul = 18
    Div = 19
    Mod = 20
    InstanceOf = 21

class LogicalConnective(object):
    def __init__(self, operator, operands):
        self.operator = operator
        self.operands = operands

    @classmethod
    def create(cls, operator, operands):
        return cls(operator, operands)

    def __and__(self, other):
        return LogicalConnective.create(Operator.And, (self, other))

    def __or__(self, other):
        return LogicalConnective.create(Operator.Or, (self, other))

def and_(a, b):
    if isinstance(a, LogicalConnective) and isinstance(b, LogicalConnective):
        return LogicalConnective(Operator.And, (a, b))
    else:
        raise TypeError('Both operands to conjunction must be '
                        'LogicalConnective objects; got {0} & {1}'.format(
                            type(a), type(b)))

def or_(a, b):
    if isinstance(a, LogicalConnective) and isinstance(b, LogicalConnective):
        return LogicalConnective(Operator.Or, (a, b))
    else:
        raise TypeError('Both operands to disjunction must be LogicalConnective '
                        'objects; got {0} | {1}'.format(type(a), type(b)))

class IdentityOperand(object):
    def __eq__(self, value):
        return LogicalConnective(Operator.Equal, (self, value))

    def __ne__(self, value):
        return LogicalConnective(Operator.NotEqual, (self, value))

class RelativeOperand(IdentityOperand):
    def __ge__(self, value):
        return LogicalConnective(Operator.GreaterEqual, (self, value))

    def __gt__(self, value):
        return LogicalConnective(Operator.Greater, (self, value))

    def __le__(self, value):
        return LogicalConnective(Operator.LessEqual, (self, value))

    def __lt__(self, value):
        return LogicalConnective(Operator.Less, (self, value))

    def between(self, left, right):
        return LogicalConnective(Operator.Between, (self, left, right))

class Operand(RelativeOperand):
    def contains(self, contained):
        return LogicalConnective(Operator.Contains, (self, contained))

    def endswith(self, trailing):
        return LogicalConnective(Operator.EndsWith, (self, trailing))

    def is_(self, value):
        """ Test if a property is null

        :param value: May (presently?) only be None
        """
        return LogicalConnective(Operator.Is, (self, value))

    def is_not(self, value):
        """ Test if a property is not nll

        :param value: May (presently?) only be None
        """
        return LogicalConnective(Operator.IsNot, (self, value))

    def like(self, value):
        return LogicalConnective(Operator.Like, (self, value))

    def matches(self, expression):
        return LogicalConnective(Operator.Matches, (self, expression))

    def startswith(self, leading):
        return LogicalConnective(Operator.StartsWith, (self, leading))

class ArithmeticMixin(object):
    def __add__(self, other):
        return ArithmeticOperation(Operator.Add, (self, other))
    def __radd__(self, left):
        return ArithmeticOperation(Operator.Add, (left, self))

    def __sub__(self, other):
        return ArithmeticOperation(Operator.Sub, (self, other))
    def __rsub__(self, left):
        return ArithmeticOperation(Operator.Sub, (left, self))

    def __mul__(self, other):
        return ArithmeticOperation(Operator.Mul, (self, other))
    def __rmul__(self, left):
        return ArithmeticOperation(Operator.Mul, (left, self))

    def __div__(self, other):
        return ArithmeticOperation(Operator.Div, (self, other))
    def __rdiv__(self, left):
        return ArithmeticOperation(Operator.Div, (left, self))

    def __mod__(self, other):
        return ArithmeticOperation(Operator.Mod, (self, other))
    def __rmod__(self, left):
        return ArithmeticOperation(Operator.Mod, (left, self))

class ArithmeticOperation(ArithmeticMixin, RelativeOperand):
    def __init__(self, operator, operands):
        self.operator = operator
        self.operands = operands
        self.paren = False

    def __getitem__(self, key):
        """Provide syntax to parenthesise an operation

        Do not see any reason to enforce a key type.
        """
        self.paren = True
        return self


# Record Attributes
class InstanceOfMixin(object):
    @classmethod
    def instanceof(cls, left, right=None):
        if cls is InstanceOf:
            return LogicalConnective(Operator.InstanceOf, (left, right))
        else: # Subclass
            return LogicalConnective(Operator.InstanceOf, (cls, left))

def instanceof(left, right):
    return InstanceOfMixin.instanceof(left, right)

