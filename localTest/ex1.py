# set an active netcat to bind on port and listen
#
# nc -Clk 2425
#


__author__ = 'Ostico'

import socket
import hexdump

sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )

try:
    sock.connect( ('localhost', 2425) )
except socket.error, e:
    print "Socket Error: %s" % e
    quit(1)

msg = ""
m = sock.recv(1)
while m != "\n":
    msg += m
    sock.send( msg + "\n" )
    m = sock.recv(1)

hexdump.hexdump( msg )

