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
    Like = 12
    Matches = 13
    StartsWith = 14

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

class Conditional(object):
    def __eq__(self, value):
        return LogicalConnective(Operator.Equal, (self, value))

    def __ge__(self, value):
        return LogicalConnective(Operator.GreaterEqual, (self, value))

    def __gt__(self, value):
        return LogicalConnective(Operator.Greater, (self, value))

    def __le__(self, value):
        return LogicalConnective(Operator.LessEqual, (self, value))

    def __lt__(self, value):
        return LogicalConnective(Operator.Less, (self, value))

    def __ne__(self, value):
        return LogicalConnective(Operator.NotEqual, (self, value))

    def between(self, left, right):
        return LogicalConnective(Operator.Between, (self, left, right))

    def contains(self, contained):
        return LogicalConnective(Operator.Contains, (self, contained))

    def endswith(self, trailing):
        return LogicalConnective(Operator.EndsWith, (self, trailing))

    def is_(self, value):
        """ Test if a property is null

        :param value: May (presently?) only be None
        """
        return LogicalConnective(Operator.Is, (self, value))

    def like(self, value):
        return LogicalConnective(Operator.Like, (self, value))

    def matches(self, expression):
        return LogicalConnective(Operator.Matches, (self, expression))

    def startswith(self, leading):
        return LogicalConnective(Operator.StartsWith, (self, leading))

