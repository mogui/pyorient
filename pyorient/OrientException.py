# @TODO extend a few more exception to be more descriptive
class PyOrientException(Exception):
  def __init__(self, message, errors):
    Exception.__init__(self, message)
    # errors is an array of tuple made this way:
    # ( java_exception_class,  message)
    self.errors = errors

  def __str__(self):
    return "%s - %s" % (self.message, self.errors[0][1])

class PyOrientConnectionException(PyOrientException):
  pass