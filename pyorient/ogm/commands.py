from ..utils import to_str

class VertexCommand(object):
    def __init__(self, command_text):
        self.command_text = command_text

    def __str__(self):
        return to_str(self.__unicode__())

    def __unicode__(self):
        return u'{}'.format(self.command_text)

class CreateEdgeCommand(object):
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
