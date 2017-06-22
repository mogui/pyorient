from ..utils import to_str
from pyorient import OrientRecordLink

class Command(object):
    pass

def create_cache_callback(graph, cache):
    if cache is None:
        return None

    def cache_cb(record):
        cache[OrientRecordLink(record._rid[1:])] = graph.element_from_record(record)
    return cache_cb

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
