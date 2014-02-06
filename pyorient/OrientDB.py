
import re
import json
import os
import struct
from OrientTypes import OrientRecordLink, OrientRecord, OrientBinaryObject
from ORecordCoder import ORecordDecoder, ORecordEncoder
from OrientException import PyOrientConnectionException, PyOrientException
import socket

# Operations
SHUTDOWN    = chr(1)
CONNECT     = chr(2)
DB_OPEN     = chr(3)

# Types Constants
BOOLEAN = 1 # Single byte: 1 = true, 0 = false
BYTE    = 2
SHORT   = 3
INT     = 4
LONG    = 5
BYTES   = 6 # Used for binary data.
STRING  = 7
RECORD  = 8


def dlog(msg):
  if os.environ['DEBUG']:
    print "[DEBUG]:: %s" % msg

class OrientSocket(object):
  """docstring for OrientSocket"""
  def __init__(self, host, port):
    self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      self.s.connect((host, port))
    except socket.error, e:
      raise PyOrientConnectionException("Socket Error: %s" % e)
    self.buffer = ''

  def readShort(self):
    return struct.unpack('!h', self.s.recv(2))[0]

  def readInt(self):
    return struct.unpack('!i', self.s.recv(4))[0]

  def readByte(self):
    return ord(self.s.recv(1))

  def readBool(self):
    return self.readByte() == 1 # 1 = true, 0 = false

  def putInt(self, num):
      self.buffer += struct.pack("!i", num)

  def putShort(self, num):
      self.buffer +=  struct.pack("!h", num)

  def putString(self, string):
    self.putInt(len(string))
    self.buffer += string

  def putByte(self, c):
    self.buffer += c

  def send(self):
    self.s.send(self.buffer)
    self.buffer = ''


#
# OrientDB
#
class OrientDB(object):
  # init
  def __init__(self, host, port, user=None, pwd=None, autoconnect=True):
    self.server = {
      'host': host,
      'port': port
    }

    self.isConnected = False
    self.session_id = -1

    # If autoconnect is false
    # or we didn't give credential we don't immediately connect
    if autoconnect and user and pwd:
      self.session_id = self.connect(user, pwd)
      if(self.session_id < 0):
        raise PyOrientConnectionException("Not connected to DB")

  #
  # Prepare a command to be sent on the connection
  # take a list of fields, it autoguess by the type for string and int
  # otherwise it expects a tuple with the right type
  #
  def callCommand(self, operation, fields):

    dlog("calling %d:%s" % (ord(operation), fields))

    # write operation
    self.conn.putByte(operation)
    # write current session
    self.conn.putInt(self.session_id)
    # iterate commands
    for field in fields:
      if isinstance(field, str):
        self.conn.putString(field)
      elif isinstance(field, int):
        self.conn.putInt(field)
      else:
        # tuple with type
        t, v = field
        if t == SHORT:
          self.conn.putShort(v)
        elif t == BYTE:
          self.conn.putByte(v)
    # end for

    # send command
    self.conn.send()

  #
  # Parse a response from the server
  # giving back the raw content of the response
  #
  def parseResponse(self, types):
    status = not self.conn.readBool() # (0=OK, 1=ERROR)
    session_id = self.conn.readInt()
    if session_id != self.session_id:
      raise PyOrientException("Wrong Session ID") # really?! is it needed?

    error = None

    if not status:
      # todo: here parse error and return it
      content = [None for t in types]
      return tuple([status, error] + content)

    content = []

    for t in types:
      if t == INT:
        content.append(self.conn.readInt())
      elif t == SHORT:
        content.append(self.conn.readShort())

    return tuple([status, error] + content)


  #
  # COMMANDS IMPLEMENTATIONS
  #
  #

  #
  # CONNECT
  #
  def connect(self, user, pwd):

    # int the connection
    self.conn = OrientSocket(self.server['host'], int(self.server['port']))

    # retrieve protocol version
    self.protocolVersion = self.conn.readShort()
    # todo: decide whether give up if protocol is not supported

    # packing command
    self.callCommand(CONNECT, ["OrientDB Python client (pyorient)", "1.0", (SHORT, 19), "", user, pwd])

    ok, error, session_id = self.parseResponse([INT])
    if not ok:
      raise PyOrientConnectionException("Error during connection, no session returned")

    dlog(session_id)
    return session_id


  def db_open(self, dbname, user, pwd):
    pass












    # def command(self, query, limit = 20, fetchplan="*:-1", async=False,  **kwargs):
    #     """docstring for command"""


    #     if async:
    #         kwargs['command_type'] = QUERY_ASYNC

    #     raw_result = _pyorient.command(query, limit, **kwargs)

    #     if kwargs.get('raw', False):
    #         return raw_result

    #     ret = []

    #     for raw_record in raw_result:
    #         parser = ORecordDecoder(raw_record)
    #         record = OrientRecord(parser.data, o_class=parser.className)
    #         ret.append(record)

    #     return ret

    # def recordload(self, cluster_id, cluster_position, **kwargs):
    #     raw_record = _pyorient.recordload(cluster_id, cluster_position, **kwargs)
    #     if kwargs.get('raw_result', False):
    #         return raw_record

    #     parser = ORecordDecoder(raw_record)

    #     record = OrientRecord(parser.data, o_class=parser.className, rid="#%d:%d" % (cluster_id, cluster_position))
    #     # @TODO missing rid and version from c api)
    #     return record


    # def recordcreate(self, cluster_id, record, **kwargs):
    #     if not isinstance(record, OrientRecord):
    #         record = OrientRecord(record)

    #     parser = ORecordEncoder(record)
    #     raw_record = parser.getRaw()
    #     ret = _pyorient.recordcreate(cluster_id, raw_record, **kwargs)

    #     return ret

