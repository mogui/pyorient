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
import _pyorient

from OrientDB import *
from OrientException import *
from OrientTypes import *

#
# Module constants
#

# cluster costants
CLUSTER_PHYSICAL     = _pyorient.CLUSTER_PHYSICAL
CLUSTER_LOGICAL      = _pyorient.CLUSTER_LOGICAL     
CLUSTER_MEMORY       = _pyorient.CLUSTER_MEMORY      
CLUSTER_DEFAULT_SIZE = _pyorient.CLUSTER_DEFAULT_SIZE

# command costants
QUERY_SYNC = _pyorient.QUERY_SYNC 
QUERY_ASYNC = _pyorient.QUERY_ASYNC

# debug levels constants
PARANOID = _pyorient.PARANOID
DEBUG    = _pyorient.DEBUG
NOTICE   = _pyorient.NOTICE
INFO     = _pyorient.INFO
NORMAL   = _pyorient.NORMAL
WARNING  = _pyorient.WARNING
CRITICAL = _pyorient.CRITICAL
FATAL    = _pyorient.FATAL
SILENT   = _pyorient.SILENT






		