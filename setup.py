#!/usr/bin/env python

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

import os

from distutils.core import setup, Extension, Command
from glob import glob
from test import getTestConfig
from unittest import TextTestRunner, TestLoader, TestSuite


class TestCommand(Command):
    description = "custom clean command that forcefully removes dist/build directories"
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):
        assert os.getcwd() == self.cwd, 'Must be in package root: %s' % self.cwd
        # os.system('rm -rf ./build ./dist')
        testfiles = []
        c = getTestConfig()

        for t in glob(os.path.join(self.cwd, 'test', '*.py')):
            if t.split('/')[-1].startswith('test_'):
                if c['canroot'] or (not t.endswith('session.py') and not t.endswith('dbroot.py')):
                    testfiles.append('.'.join(
                        ['test', os.path.splitext(os.path.basename(t))[0]]))
        tests = TestLoader().loadTestsFromNames(testfiles)
        TextTestRunner(verbosity=2).run(tests)


class CleanCommand(Command):
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):
        assert os.getcwd() == self.cwd, 'Must be in package root: %s' % self.cwd
        os.system('rm -rf ./build ./dist _pyorient.so ./*.pyc ./test/*.pyc')


setup(
    name='pyorient',
    version='0.1.0',
    author='Niko Usai',
    author_email='mogui83@gmail.com',
    url='http://mogui.it',
    description='OrientDB client liborient wrapper',
    long_description=open('DESCRIPTION').read(),
    license='LICENSE',
    cmdclass={'test': TestCommand, 'clean': CleanCommand},
    packages=['pyorient'],
    ext_modules=[Extension(
        '_pyorient',
        ['src/pyorientmodule.c'],
        libraries=['orient'],
    )]
)
