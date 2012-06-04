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

class CommandTestCase(unittest.TestCase):
    """ Command Test Case """
    ordb = None
    c = getTestConfig()
    dbname = c['existing_db']

    def setUp(self):
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'], self.c['useru'],self.c['userp'])
        self.ordb.dbopen(self.dbname)

    def tearDown(self):
       try:
           self.ordb.dbclose()
       except PyOrientException, e:
           print("tryed close db but: (PyOrientException) %s" % e)
       self.ordb.close()

    def test_command_select_singlerecord_raw(self):
        result = self.ordb.command("SELECT FROM #4:0", raw=True)
        print("\n\traw result is  %s" % result)
        self.assertTrue(isinstance(result[0], str), "Error in raw retrieve error %s" % result)

    def test_command_select_singlerecord(self):
        result = self.ordb.command("SELECT FROM #4:0")
        print("\n\t%s" % result[0].__dict__)
        self.assertEqual(len(result), 1, "Error in command error %s" % result)

    def test_command_select_set(self):
        result = self.ordb.command("select from ORole")
        print("\n\tresult set is  %d length" % len(result))
        self.assertTrue(len(result) >= 0, "Error in command error %s" % result)

    def test_command_select_set_with_limit(self):
        result = self.ordb.command("select from ORole", limit=2)
        print("\n\tresult set is  %d length" % len(result))
        self.assertEqual(len(result), 2, "Error in command error %s" % result)
        result = self.ordb.command("select from ORole", 3)
        print("\tresult set is  %d length" % len(result))
        self.assertEqual(len(result), 3, "Error in command error %s" % result)
        
    
