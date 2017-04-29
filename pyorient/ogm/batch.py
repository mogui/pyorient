from .broker import get_broker
from .commands import VertexCommand

from .vertex import VertexVector

import re
import string

class Batch(object):
    READ_COMMITTED = 0
    REPEATABLE_READ = 1

    def __init__(self, graph, isolation_level=READ_COMMITTED):
        self.graph = graph
        self.objects = {}
        self.variables = {}

        if isolation_level == Batch.REPEATABLE_READ:
            self.commands = 'BEGIN ISOLATION REPEATABLE_READ\n'
        else:
            self.commands = 'BEGIN\n'

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
        command = str(value)
        if isinstance(key, slice):
            self.commands += '{}\n'.format(command)
        else:
            key = Batch.clean_name(key) if Batch.clean_name else key

            self.commands += 'LET {} = {}\n'.format(key, command)

            VarType = BatchVariable
            if isinstance(value, VertexCommand):
                VarType = BatchVertexVariable
            self.variables[key] = VarType('${}'.format(key), value)

    def sleep(self, ms):
        self.commands += 'sleep {}'.format(ms)

    def clear(self):
        self.objects.clear()
        self.variables.clear()

        self.commands = self.commands[:self.commands.index('\n') + 1]

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
                    self.commands += \
                        'COMMIT RETRY {}\nRETURN {}'.format(key.step, returned)
                else:
                    self.commands += 'COMMIT RETRY {}'.format(key.step)
            elif key.stop:
                # No commit.

                if Batch.clean_name:
                    return self.variables[Batch.clean_name(key.stop)]
                elif any(c in Batch.INVALID_CHARS for c in key.stop):
                    raise ValueError(
                        'Variable name \'{}\' contains invalid character(s).'
                            .format(key.stop))

                return self.variables[key.stop]
            else:
                if key.start:
                    returned = Batch.return_string(key.start)
                    self.commands += 'COMMIT\nRETURN {}'.format(returned)
                else:
                    self.commands += 'COMMIT'
        else:
            returned = Batch.return_string(key)
            self.commands += 'COMMIT\nRETURN {}'.format(returned)

        g = self.graph
        if returned:
            response = g.client.batch(self.commands)
            self.clear()

            if returned[0] in ('[','{'):
                return g.elements_from_records(response) if response else None
            else:
                return g.element_from_record(response[0]) if response else None
        else:
            g.client.batch(self.commands)
            self.clear()

    def commit(self, retries=None):
        """Commit batch with no return value."""
        self.commands += 'COMMIT' + (' RETRY {}'.format(retries) if retries else '')

        g = self.graph
        g.client.batch(self.commands)
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

    INVALID_CHARS = set(string.punctuation + string.whitespace)

    @staticmethod
    def default_name_cleaner(name):
        rx = '[' + re.escape(''.join(Batch.INVALID_CHARS)) + ']'
        return re.sub(rx, '_', name)

    clean_name = None
    @classmethod
    def use_name_cleaner(cls, cleaner=default_name_cleaner):
        cls.clean_name = cleaner

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

class BatchVariable(object):
    def __init__(self, reference, value):
        self._id = reference
        self.value = value

class BatchVertexVariable(BatchVariable):
    def __init__(self, reference, value):
        super(BatchVertexVariable, self).__init__(reference, value)

    def __call__(self, edge_or_broker):
        if hasattr(edge_or_broker, 'broker'):
            edge_or_broker = edge_or_broker.broker.element_cls
        elif hasattr(edge_or_broker, 'element_cls'):
            edge_or_broker = edge_or_broker.element_cls

        if edge_or_broker.decl_type == 1:
            return BatchVertexVector(self, edge_or_broker.objects)

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


