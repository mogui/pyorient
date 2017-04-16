import unittest
import pyorient

# db_name = "GratefulDeadConcerts"
# client = pyorient.OrientDB("localhost", 2424)
# client.set_session_token(True)
# cluster_info = client.db_open( db_name, "admin", "admin" )
# print(client.db_count_records())

__author__ = 'Ostico <ostico@gmail.com>'


class OrientVersionTestCase( unittest.TestCase ):
    """ Orient Version Test Case """

    def test_string1(self):
        release = "2.2.0-rc1"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert isinstance( x.subversion, str )
        assert x.major == 2
        assert x.minor == 2
        assert x.build == 0
        assert x.subversion == "rc1"

    def test_string2(self):
        release = "1.10.1"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert x.major == 1
        assert x.minor == 10
        assert x.build == 1
        assert x.subversion is ''

    def test_string3(self):
        release = "2.0.19-rc2"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert isinstance( x.subversion, str )
        assert x.major == 2
        assert x.minor == 0
        assert x.build == 19
        assert x.subversion == "rc2"

    def test_string4(self):
        release = "2.2.0 ;Unknown (build 0)"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert isinstance( x.subversion, str )
        assert x.major == 2
        assert x.minor == 2
        assert x.build == 0
        assert x.subversion == ";Unknown (build 0)"

    def test_string5(self):
        release = "2.2-rc1 ;Unknown (build 0)"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert isinstance( x.subversion, str )
        assert x.major == 2
        assert x.minor == 2
        assert x.build == 0
        assert x.subversion == "rc1 ;Unknown (build 0)"

    def test_string6(self):
        release = "v2.2"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert isinstance( x.subversion, str )
        assert x.major == 2
        assert x.minor == 2
        assert x.build == 0
        assert x.subversion == ""

    def test_string_version2(self):
        release = "2.2.0 (build develop@r79d281140b01c0bc3b566a46a64f1573cb359783; 2016-05-18 14:14:32+0000)"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert isinstance( x.subversion, str )
        assert x.major == 2
        assert x.minor == 2
        assert x.build == 0
        assert x.subversion == "(build develop@r79d281140b01c0bc3b566a46a64f1573cb359783; 2016-05-18 14:14:32+0000)"

    def test_new_string(self):
        release = "OrientDB Server v2.2.0 (build develop@r79d281140b01c0bc3b566a46a64f1573cb359783; 2016-05-18 14:14:32+0000)"
        x = pyorient.OrientVersion(release)
        assert isinstance( x.major, int )
        assert isinstance( x.minor, int )
        assert isinstance( x.build, int )
        assert isinstance( x.subversion, str )
        assert x.major == 2
        assert x.minor == 2
        assert x.build == 0
        assert x.subversion == "(build develop@r79d281140b01c0bc3b566a46a64f1573cb359783; 2016-05-18 14:14:32+0000)"
