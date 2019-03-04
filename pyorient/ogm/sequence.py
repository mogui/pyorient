from .what import What, ChainableWhat
from .property import PreOp

class Sequences(object):
    def __init__(self, graph):
        self._graph = graph
        self._library = {}

    def __contains__(self, name):
        return name in self._library

    def create(self, name, seq_type, start=None, inc=None, cache=None):
        """Create a new sequence.
        :param name: Sequence's name
        :param seq_type: Either Sequence.Ordered or Sequence.Cached
        :param start: Sequence starting value
        :param inc: Increment with each run of next() on the sequence
        :param cache: Number of values to pre-cache, when seq_type is Sequence.Cached
        """
        if seq_type is Sequence.Cached:
            seq_type = 'CACHED'
            cache = ' CACHE {}'.format(cache) if cache is not None else ''
        else:
            seq_type = 'ORDERED'
            cache = ''

        self._graph.client.command(
            'CREATE SEQUENCE {} TYPE {}{}{}{}'.format(
                name,
                seq_type,
                ' START {}'.format(start) if start is not None else '',
                ' INCREMENT {}'.format(inc) if inc is not None else '',
                cache
                ))
        self._library[name] = seq = Sequence(name)
        return seq

    def drop(self, sequence):
        name = '{}'.format(sequence)
        self._graph.client.command('DROP SEQUENCE ' + name)
        del self._library[name]

class NewSequence(PreOp):
    def __init__(self, seq_type, start=None, inc=None, cache=None):
        self.seq_type = seq_type
        self.start = start
        self.inc = inc
        self.cache = cache

    def __call__(self, graph, attr):
        graph.sequences.create(attr, self.seq_type, self.start, self.inc, self.cache)

class Sequence(ChainableWhat):
    Ordered = 0
    Cached = 1

    def __init__(self, name):
        super(Sequence, self).__init__([(What.Sequence, (name, ))], [])

    def __format__(self, _):
        return self._chain[0][1][0]

    def current(self):
        return ChainableWhat(self._chain + [(What.Current, )], [])

    def next(self):
        return ChainableWhat(self._chain + [(What.Next, )], [])

def sequence(name):
    return Sequence(name)

