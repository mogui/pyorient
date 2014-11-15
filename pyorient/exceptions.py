# @TODO extend a few more exception to be more descriptive
class PyOrientException(Exception):
    def __init__(self, message, errors):
        Exception.__init__(self, message)
        # errors is an array of tuple made this way:
        # ( java_exception_class,  message)
        self.errors = errors

    def __str__(self):
        if self.errors:
            return "%s - %s" % (Exception.__str__(self), self.errors[0][1])
        else:
            return Exception.__str__(self)


class PyOrientConnectionException(PyOrientException):
    pass


class PyOrientDatabaseException(PyOrientException):
    pass


class PyOrientCommandException(PyOrientException):
    pass


class PyOrientBadMethodCallException(PyOrientException):
    pass


class PyOrientWrongProtocolVersionException(PyOrientException):
    pass


class PyOrientSerializationException(PyOrientException):
    pass