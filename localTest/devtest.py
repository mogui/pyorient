# Copyright 2012 Niko Usai <usai.niko@gmail.com>, http://mogui.it
#
#   this file is part of pyorient
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import os
import sys

os.environ['DEBUG'] = "1"

if os.path.realpath( '../' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '../' ) )

if os.path.realpath( '.' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '.' ) )


from pyorient import OrientDB, PyOrientException
#from . import getTestConfig

#c = getTestConfig()
#dbname = c['existing_db']


def test_simpleconnection():
    db = OrientDB('127.0.0.1', 2424, "admin", "admin")
    assert db.conn.get_session_id() > 0


def test_wrongconnect():
    db = OrientDB( '127.0.0.1', 2424 )
    try:
        db.connect( "root", "asder" )
    except PyOrientException, e:
        # print "Exc: %s" % e
        assert True
    else:
        assert False


# def test_shutdown():.ClassName
#   db = OrientDB('127.0.0.1', 2424, "root", "root")
#   db.shutdown("root", "root")


def test_dbopen():
    try:
        db = OrientDB( '127.0.0.1', 2424, "admin", "admin" )
        e = db.db_exists( "GratefulDeadConcerts" )
        if e:
            e = db.db_open( "GratefulDeadConcerts", "admin", "admin" )

        # print "%r" % e
    except Exception as e:
        print "%r" % type(e)
        print "%r" % e.message
        quit(0)
    assert e


def test_reload():
    db = OrientDB( '127.0.0.1', 2424, "admin", "admin" )
    ret = db.db_open( "GratefulDeadConcerts", "admin", "admin" )
    assert ret
    ret = db.db_reload()
    # print "%r" % ret
    assert ret[0]


def test_create_destroy():
    db = OrientDB( '127.0.0.1', 2424, "admin", "admin" )

    if db.db_exists( "GratefulDeadConcerts" ):
        ret = db.db_drop('mock_db')
        assert (ret >= 0)

    ret = db.db_create('mock_db')
    assert (ret >= 0)
    ret = db.db_drop('mock_db')
    assert (ret >= 0)


def test_select():
    try:
        db = OrientDB( '127.0.0.1', 2424, "admin", "admin" )
        e = db.db_exists( "GratefulDeadConcerts" )
        if e:
            e = db.db_open( "GratefulDeadConcerts", "admin", "admin" )

        db.command( "select * from Person" )

        # print "%r" % e
    except Exception as e:
        print "%r" % type(e)
        print "%r" % e.message
        quit(0)
    assert e

test_simpleconnection()
# test_dbopen()
# test_wrongconnect()
# test_create_destroy()
# test_reload()
test_select()
