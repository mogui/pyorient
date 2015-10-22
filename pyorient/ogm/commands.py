class CreateVertexCommand(object):
    def __init__(self, command_text):
        self.command_text = command_text

    def __str__(self):
        return '{}'.format(self.command_text)


class CreateEdgeCommand(object):
    def __init__(self, command_text):
        self.command_text = command_text
        self.retries = None

    def __str__(self):
        if self.retries:
            return '{} RETRY {}'.format(self.command_text, self.retries)
        else:
            return '{}'.format(self.command_text)

    def retry(self, retries):
        self.retries = retries
        return self
