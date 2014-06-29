import os


def is_debug_active():
    if 'DEBUG' in os.environ:
        if os.environ['DEBUG'].lower() in ( '1', 'true' ):
            return True
    return False


def dlog( msg ):
    # add check for DEBUG key because KeyError Exception is not caught
    # and if no DEBUG key is set, the driver crash with no reason when
    # connection starts
    if is_debug_active():
        print "[DEBUG]:: %s" % msg
