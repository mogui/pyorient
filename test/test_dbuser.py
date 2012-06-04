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
import time
from . import getTestConfig

class DbUserTestCase(unittest.TestCase):
    """ main test case """
    ordb = None
    c = getTestConfig()
    dbname = c['existing_db']
    
    def setUp(self):
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'], self.c['useru'],self.c['userp'])

    def tearDown(self):
        try:
            self.ordb.dbclose()
        except pyorient.PyOrientException, e:
            print("tryed close db but: (PyOrientException) %s" % e)
        
        self.ordb.close()
              
    def test_dbopen_existing(self):
        ret = self.ordb.dbopen(self.dbname)
        self.assertEqual(ret, 0, "Db not opened error %d" % ret)
        
    def test_dbopen_existing_inline(self):
        ret = self.ordb.dbopen(self.dbname, self.c['useru'],self.c['userp'])
        self.assertEqual(ret, 0, "Db not opened with inline credential error %d" % ret)

    def test_dbopen_existing_inline_wrong(self):
        self.assertRaises(pyorient.PyOrientException, self.ordb.dbopen , self.dbname, "adminassd", "asdadmin")

    def test_dbsize(self):
        ret = self.ordb.dbopen(self.dbname)
        dim = self.ordb.dbsize()
        self.assertTrue(dim >= 0, "Size returned negative value %d"% dim)

    def test_dbcountrecords(self):
        ret = self.ordb.dbopen(self.dbname)
        recordnum = self.ordb.dbcountrecords()
        if recordnum > 0:
            print("\n\tNumber of records in DB %d" % recordnum)
        self.assertTrue(recordnum >= 0, "Number of records couldn't be negative")

    def test_dbreload(self):
        ret = self.ordb.dbopen(self.dbname)
        ret = self.ordb.dbreload()
        self.assertEqual(ret, 0, "Problem in reloading database")
