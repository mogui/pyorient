# little test with decorators

def entryExit(f):
    def new_f():
        print "Entering", f.__name__
        f()
        print "Exited", f.__name__

    return new_f


@entryExit
def func1():
    print "inside func1()"


@entryExit
def func2():
    print "inside func2()"


func1()
func2()
print func1.__name__
