# @TODO extend a few more exception to be more descriptive
class PyOrientException(Exception):
    def __init__(self, message, errors):

        _errorClass = message.split( "." )[-1]

        x = {
            "OCommandSQLParsingException": PyOrientSQLParsingException,
            "ODatabaseException": PyOrientDatabaseException,
            "OConfigurationException": PyOrientDatabaseException,
            "OCommandExecutorNotFoundException": PyOrientCommandException,
            "OSecurityAccessException": PyOrientSecurityAccessException,
            "ORecordDuplicatedException": PyOrientORecordDuplicatedException,
            "OSchemaException": PyOrientSchemaException,
            "OIndexException": PyOrientIndexException
        }

        # Override the exception Type with OrientDB exception map
        if _errorClass in x.keys():
            self.__class__ = x[ _errorClass ]

        Exception.__init__(self, message)
        # errors is an array of tuple made this way:
        # ( java_exception_class,  message)
        self.errors = errors

    def __str__(self):
        if self.errors:
            return "%s - %s" % (Exception.__str__(self), self.errors[0])
        else:
            return Exception.__str__(self)


class PyOrientConnectionException(PyOrientException):
    pass


class PyOrientConnectionPoolException(PyOrientException):
    pass


class PyOrientSecurityAccessException(PyOrientException):
    pass


class PyOrientDatabaseException(PyOrientException):
    pass


class PyOrientSQLParsingException(PyOrientException):
    pass


class PyOrientCommandException(PyOrientException):
    pass

class PyOrientSchemaException(PyOrientException):
    pass

class PyOrientIndexException(PyOrientException):
    pass

class PyOrientORecordDuplicatedException(PyOrientException):
    pass


class PyOrientBadMethodCallException(PyOrientException):
    pass


class PyOrientWrongProtocolVersionException(PyOrientException):
    pass


class PyOrientSerializationException(PyOrientException):
    pass


class PyOrientNullRecordException(PyOrientException):
    pass
