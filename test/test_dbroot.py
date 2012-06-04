#   Copyright 2012 Niko Usai <usai.niko@gmail.com>, http://mogui.it
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

import pyorient
import unittest
import time
from . import getTestConfig

class DbRootTestCase(unittest.TestCase):
    ordb = None
    c = getTestConfig()
    dbname = c['new_db']
    
    def setUp(self):
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'], self.c['rootu'],self.c['rootp'])
        try:
            ret = self.ordb.dbdelete(self.dbname)
        except:
            pass

    def tearDown(self):
        self.ordb.close()
        try:
            ret = self.ordb.dbdelete(self.dbname)
        except:
            pass
        
    def test_dbcreate(self):
        ret = self.ordb.dbcreate(self.dbname)
        self.assertTrue(ret >= 0, "Db not created error %s" % ret)
        ret = self.ordb.dbdelete(self.dbname)
        
    def test_dbexists(self):
        self.ordb.dbcreate(self.dbname)
        ret = self.ordb.dbexists(self.dbname)
        self.assertEqual(ret, 1, "Db does not exists error %d" % ret)
        ret2 = self.ordb.dbexists("fake_db")
        self.assertEqual(ret2, 0, "Fake Db exists error %d" % ret2)
        self.ordb.dbdelete(self.dbname)

    def test_dbdelete(self):
        ret = self.ordb.dbcreate(self.dbname)
        self.assertEqual(ret, 0, "Db not cdeleted error %d" % ret)
        ret = self.ordb.dbdelete(self.dbname)
