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
try:
    import configparser
except ImportError:
    import ConfigParser as configparser


def getTestConfig():
    config = configparser.RawConfigParser()
    config.read('tests.cfg')

    # getfloat() raises an exception if the value is not a float
    # getint() and getboolean() also do this for their respective types
    conf = {
        'host': config.get('server', 'host'),
        'port': config.get('server', 'port'),
        'canroot': config.getboolean('server', 'canroot'),
        'rootu': config.get('root', 'user'),
        'rootp': config.get('root', 'pwd'),
        'useru': config.get('user', 'user'),
        'userp': config.get('user', 'pwd'),
        'existing_db': config.get('db', 'existing'),
        'new_db': config.get('db', 'new'),
    }
    return conf
