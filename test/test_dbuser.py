from __future__ import print_function

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

class DbUserTestCase(unittest.TestCase):
    """ main test case """
    ordb = None
    c = getTestConfig()
    dbname = c['existing_db']
    
    def setUp(self):
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'])

    def tearDown(self):
        try:
            self.ordb.db_close()
        except pyorient.PyOrientException, e:
            print("tryed close db but: (PyOrientException) %s" % e)

    def test_dbopen_existing(self):
        ret = self.ordb.db_open(self.dbname, self.c['useru'], self.c['userp'])
        assert len(ret) > 0

    def test_dbopen_existing_inline(self):
        ret = self.ordb.db_open(self.dbname, self.c['useru'], self.c['userp'])
        assert len(ret) > 0

    def test_dbopen_existing_inline_wrong(self):
        self.assertRaises(pyorient.PyOrientException, self.ordb.db_open , self.dbname, "adminassd", "asdadmin")

    def test_dbsize(self):
        ret = self.ordb.db_open(self.dbname,self.c['useru'], self.c['userp'])
        dim = self.ordb.db_size()
        self.assertTrue(dim >= 0, "Size returned negative value %d"% dim)

    def test_dbcountrecords(self):
        ret = self.ordb.db_open(self.dbname, self.c['useru'], self.c['userp'])
        recordnum = self.ordb.db_count_records()
        if recordnum > 0:
            print("\n\tNumber of records in DB %d" % recordnum)
        self.assertTrue(recordnum >= 0, "Number of records couldn't be negative")

    def test_dbreload(self):
        ret = self.ordb.db_open(self.dbname, self.c['useru'], self.c['userp'])
        assert ret
        ret = self.ordb.db_reload()
        assert ret[0]  # , "Problem in reloading database")
