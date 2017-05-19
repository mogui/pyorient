from .broker import get_broker
from .commands import Command, VertexCommand, CreateEdgeCommand

from .vertex import VertexVector
from .what import What, LetVariable, VertexWhatMixin, EdgeWhatMixin

from .expressions import ExpressionMixin
from .query_utils import ArgConverter

import re
import string
from copy import copy

class Batch(ExpressionMixin):
    READ_COMMITTED = 0
    REPEATABLE_READ = 1

    def __init__(self, graph, isolation_level=READ_COMMITTED):
        self.graph = graph
        self.objects = {}
        self.variables = {}
        self.stack = [[]]

        if isolation_level == Batch.REPEATABLE_READ:
            self.stack[0].append('BEGIN ISOLATION REPEATABLE_READ')
        else:
            self.stack[0].append('BEGIN')

        for name,cls in graph.registry.items():
            broker = get_broker(cls)
            if broker:
                self.objects[cls] = broker = BatchBroker(broker)
            else:
                self.objects[cls] = broker = BatchBroker(cls.objects)

            broker_name = getattr(cls, 'registry_plural', None)
            if broker_name is not None:
                setattr(self, broker_name, broker)

    def __setitem__(self, key, value):
        """Add a command to the batch.
        :param key: A name for the variable storing the results of the command,
        or an empty slice if command is only meant for its side-effects.
        Names can be reused.
        :param value: The command to perform.
        """
        if isinstance(key, slice):
            command = str(value)
            self.stack[-1].append('{}'.format(command))
        else:
            if isinstance(value, Command):
                command = str(value)
            else:
                command = ArgConverter.convert_to(ArgConverter.Vertex, value, self)

            key = Batch.clean_name(key) if Batch.clean_name else key

            self.stack[-1].append('LET {} = {}'.format(key, command))

            VarType = BatchVariable
            if isinstance(value, VertexCommand):
                VarType = BatchVertexVariable
            elif isinstance(value, CreateEdgeCommand):
                VarType = BatchEdgeVariable
            self.variables[key] = VarType('${}'.format(key), value)

    def sleep(self, ms):
        """Put the batch in wait.
        :param ms: Number of milliseconds.
        """
        self.stack[-1].append('sleep {}'.format(ms))

    def clear(self):
        """Clear the batch for a new set of commands."""
        # TODO Give option to reuse batches?
        self.variables.clear()

        # Stack size should be 1
        self.stack[0] = self.stack[0][:1]

    def if_(self, condition):
        """Conditional execution in a batch.
        :param condition: Anything that can be passed to Query.filter()
        """
        return BatchBranch(self, condition)

    def __str__(self):
        return u'\n'.join(self.stack[-1])

    def __getitem__(self, key):
        """Commit batch with return value, or reference a previously defined
        variable.

        Using a plain string as a key commits and returns the named variable.

        Slicing with only a 'stop' value does not commit - it is the syntax for
        using a variable. Otherwise slicing can give finer control over commits;
        step values give a retry limit, and a start value denotes the returned
        variable.
        """

        returned = None
        if isinstance(key, slice):
            if key.step:
                if key.start:
                    returned = Batch.return_string(key.start)
                    self.stack[-1].append(
                        'COMMIT RETRY {}\nRETURN {}'.format(key.step, returned))
                else:
                    self.stack[-1].append('COMMIT RETRY {}'.format(key.step))
            elif key.stop:
                # No commit.

                if Batch.clean_name:
                    return copy(self.variables[Batch.clean_name(key.stop)])
                elif any(c in Batch.INVALID_CHARS for c in key.stop) or key.stop[0].isdigit():
                    raise ValueError(
                        'Variable name \'{}\' contains invalid character(s).'
                            .format(key.stop))

                return copy(self.variables[key.stop])
            else:
                if key.start:
                    returned = Batch.return_string(key.start)
                    self.stack[-1].append('COMMIT\nRETURN {}'.format(returned))
                else:
                    self.stack[-1].append('COMMIT')
        else:
            returned = Batch.return_string(key)
            self.stack[-1].append('COMMIT\nRETURN {}'.format(returned))

        g = self.graph
        commands = str(self)
        if returned:
            response = g.client.batch(commands)
            self.clear()

            if returned[0] in ('[','{'):
                return g.elements_from_records(response) if response else None
            else:
                return g.element_from_record(response[0]) if response else None
        else:
            g.client.batch(commands)
            self.clear()

    def commit(self, retries=None):
        """Commit batch with no return value."""
        self.stack[-1].append('COMMIT' + (' RETRY {}'.format(retries) if retries else ''))

        g = self.graph
        g.client.batch(str(self))
        self.clear()

    @staticmethod
    def return_string(variables):
        cleaned = Batch.clean_name or (lambda s:s)

        if isinstance(variables, (list, tuple)):
            return '[' + ','.join(
                '${}'.format(cleaned(var)) for var in variables) + ']'
        elif isinstance(variables, dict):
            return '{' + ','.join(
                '{}:${}'.format(repr(k),cleaned(v))
                    for k,v in variables.items()) + '}'
        else:
            # Since any value can be returned from a batch,
            # '$' must be used when a variable is referenced
            if isinstance(variables, str):
                if variables[0] == '$':
                    return '{}'.format('$' + cleaned(variables[1:]))
                else:
                    return repr(variables)
            else:
                return '{}'.format(variables)

    INVALID_CHARS = frozenset(''.join(c for c in string.punctuation if c is not '_') + string.whitespace)

    @staticmethod
    def default_name_cleaner(name):
        # Can't begin with a digit
        rx = r'^\d|[' + re.escape(''.join(Batch.INVALID_CHARS)) + r']'
        return re.sub(rx, '_', name)

    clean_name = None
    @classmethod
    def use_name_cleaner(cls, cleaner=default_name_cleaner):
        cls.clean_name = cleaner

class BatchBranch():
    IF = 'if ({}) {{\n  {}\n}}'
    def __init__(self, batch, condition):
        self.batch = batch
        self.condition = condition

    def __enter__(self):
        self.batch.stack.append([])

    def __exit__(self, e_type, e_value, e_trace):
        batch = self.batch
        branch_commands = '\n'.join(batch.stack.pop())

        if e_type is not None:
            # If an exception was raised, abort the batch
            batch.stack[-1].append('ROLLBACK')

        batch.stack[-1].append(
            BatchBranch.IF.format(
                    ArgConverter.convert_to(ArgConverter.Boolean, self.condition, batch),
                    branch_commands
                ))

class RollbackException(Exception):
    pass

class BatchBroker(object):
    def __init__(self, broker):
        self.broker = broker

    def __getattribute__(self, name):
        suffix = '_command'
        if name == 'broker':
            return super(BatchBroker, self).__getattribute__(name)
        elif name.endswith(suffix):
            return self.broker.__getattribute__(name)
        else:
            return self.broker.__getattribute__(name + suffix)

class BatchVariable(LetVariable):
    def __init__(self, reference, value):
        super(BatchVariable, self).__init__(reference[1:])
        self._id = reference
        self._value = value

    def __copy__(self):
        return type(self)(self._id, self._value)

class BatchVertexVariable(BatchVariable, VertexWhatMixin):
    def __init__(self, reference, value):
        super(BatchVertexVariable, self).__init__(reference, value)

    def __call__(self, edge_or_broker):
        if hasattr(edge_or_broker, 'broker'):
            edge_or_broker = edge_or_broker.broker.element_cls
        elif hasattr(edge_or_broker, 'element_cls'):
            edge_or_broker = edge_or_broker.element_cls

        if edge_or_broker.decl_type == 1:
            return BatchVertexVector(self, edge_or_broker.objects)

class BatchEdgeVariable(BatchVariable, EdgeWhatMixin):
    def __init__(self, reference, value):
        super(BatchEdgeVariable, self).__init__(reference, value)

class BatchVertexVector(VertexVector):
    def __init__(self, origin, edge_broker, **kwargs):
        super(BatchVertexVector, self).__init__(origin, edge_broker, **kwargs)

    def __gt__(self, target):
        """Syntactic sugar for creating an edge in a batch."""
        if hasattr(target, '_id'):
            return self.edge_broker.create_command(
                self.origin, target, **self.kwargs)
        return self

    def __lt__(self, origin):
        """Syntactic sugar for creating an edge in a batch.

        Convenient when 'origin' vertex defined outside batch.
        """
        if hasattr(origin, '_id'):
            return self.edge_broker.create_command(
                origin
                , self.origin # Target
                , **self.kwargs)
        return self


