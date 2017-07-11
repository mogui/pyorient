from ..utils import to_str
from pyorient import OrientRecordLink

class Command(object):
    pass

def create_cache_callback(graph, cache):
    if cache is None:
        return None

    def cache_cb(record):
        cache[OrientRecordLink(record._rid[1:])] = graph.element_from_record(record, cache)
    return cache_cb

class CacheMixin(object):
    """Manages a cache dictionary and the callback for adding to it
    Assumes inheriting classes specify:
        - 'graph' variable or property
    """
    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache):
        self._cacher = create_cache_callback(self.graph, cache)
        self._cache = cache

    @property
    def cacher(self):
        return self._cacher

class VertexCommand(Command):
    def __init__(self, command_text):
        self.command_text = command_text

    def __str__(self):
        return to_str(self.__unicode__())

    def __unicode__(self):
        return u'{}'.format(self.command_text)

class CreateEdgeCommand(Command):
    def __init__(self, command_text):
        self.command_text = command_text
        self.retries = None

    def __str__(self):
        return to_str(self.__unicode__())

    def __unicode__(self):
        if self.retries:
            return u'{} RETRY {}'.format(self.command_text, self.retries)
        else:
            return u'{}'.format(self.command_text)

    def retry(self, retries):
        self.retries = retries
        return self

from .expressions import ExpressionMixin
from .property import PropertyEncoder
from .operators import LogicalConnective
class RetrievalCommand(Command, ExpressionMixin):
    def __init__(self, command_text=None):
        self._compiled = command_text

    def compile(self, compiler=None):
        """Compile this command for reuse later.
        :return: The compiled command text
        :param compiler: (Optional) A function that compiles the command.
        This function must not trigger __str__ on the command instance.
        """
        if not self._compiled:
            self._compiled = compiler() if compiler else str(self)
        return self._compiled

    def purge(self):
        """Purge the results of a previous compilation."""
        self._compiled = None

    def __str__(self):
        """The compiled command text."""
        return self._compiled or ''

    def FORMAT_ENCODER(self, v):
        if isinstance(v, RetrievalCommand):
            return '({})'.format(v.compile()) 
        elif isinstance(v, LogicalConnective):
            return self.filter_string(v)
        else:
            return PropertyEncoder.encode_value(v, self)

    def format(self, *args, **kwargs):
        """Return a copy of the raw command (not usable directly), where {}'s
        of the compiled command are replaced by positional or keyword arguments,
        similar to Python's str.format()
        :param args: The n'th argument is substituted for the {n}th token.
        :param kwargs: Substitute tokens by keyword.
        """
        encode = self.FORMAT_ENCODER
        return RetrievalCommand(self.compile().format(*[encode(arg) for arg in args], **{k:encode(v) for k,v in kwargs.items()}))

