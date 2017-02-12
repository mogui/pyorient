import os
import re

from pyorient.serializations import OrientSerialization

try:
    from urllib.parse import urlparse, urlunparse
except ImportError:
    from urlparse import urlparse, urlunparse

class Config(object):
    """Specifies how to connect to OrientDB server."""
    def __init__(self, host, port, user, cred, db_name=None, storage='memory'
                 , initial_drop=False, serialization_type=OrientSerialization.CSV):
        """
        :param initial_drop: Useful for testing; signal that any existing
        database with this configuration should be dropped on connect.
        """
        self.host = host or 'localhost'
        self.port = port or 2424
        self.user = user
        self.cred = cred
        self.db_name = db_name
        self.storage = storage
        self.initial_drop = initial_drop
        self.serialization_type = serialization_type
        self.scripts = None

    @classmethod
    def from_url(cls, url, user, cred, initial_drop=False, serialization_type=OrientSerialization.CSV):
        url_exp = re.compile(r'^(\w+:\/\/)?(.*)')
        url_match = url_exp.match(url)
        if not url_match.group(1):
            if '/' in url:
                url = 'plocal://' + url
            else:
                url = 'memory://' + url

        url_parts = urlparse(url)

        if url_parts.path:
            db_name = os.path.basename(url_parts.path)
            return cls(url_parts.hostname, url_parts.port, user, cred, db_name
                       , url_parts.scheme, initial_drop, serialization_type)
        else:
            db_name = url_parts.netloc
            return cls(None, url_parts.port, user, cred, db_name
                       , url_parts.scheme, initial_drop, serialization_type)

    def set_database(self, db_name, storage):
        self.db_name = db_name
        self.storage = storage

    def set_scripts(self, scripts):
        self.scripts = scripts

