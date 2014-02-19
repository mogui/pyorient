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
from . import getTestConfig

class DbRootTestCase(unittest.TestCase):
    def setUp(self):
        self.c = getTestConfig()
        self.dbname = self.c['new_db']
        self.existing = self.c['existing_db']
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'], self.c['rootu'], self.c['rootp'])
        try:
            self.ordb.db_drop(self.dbname)
        except:
            pass

    def tearDown(self):
        try:
            self.ordb.db_close()
            self.ordb.db_drop(self.dbname)
        except:
            pass

    def test_dbcreate(self):
        ret = self.ordb.db_create(self.dbname)
        self.assertTrue(ret >= 0, "Db not created error %s" % ret)
        ret = self.ordb.db_drop(self.dbname)

    def test_dbexists(self):

        ret = self.ordb.db_exists(self.existing)
        self.assertTrue(ret, "Db does not exists error")
        ret2 = self.ordb.db_exists("fake_db")
        self.assertFalse(ret2, "Fake Db exists error")

    def test_dbdelete(self):
        self.ordb.db_create(self.dbname)
        self.ordb.db_drop(self.dbname)
        ex = self.ordb.db_exists(self.dbname)
        self.assertFalse(ex, "Db not deleted error")

