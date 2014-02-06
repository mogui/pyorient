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

__author__ = 'mogui'
import base64

from OrientDB import *
from OrientException import *
from OrientTypes import *


# cluster costants
CLUSTER_PHYSICAL     = "PHYSICAL"
CLUSTER_LOGICAL      = "LOGICAL"
CLUSTER_MEMORY       = "MEMORY"
CLUSTER_DEFAULT_SIZE = -1

# Commands costants
QUERY_SYNC  = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD   = "com.orientechnologies.orient.core.sql.OCommandSQL"

# Debug levels constants
PARANOID = 9
DEBUG    = 8
NOTICE   = 7
INFO     = 6
NORMAL   = 5
WARNING  = 4
CRITICAL = 3
FATAL    = 2
SILENT   = 1






