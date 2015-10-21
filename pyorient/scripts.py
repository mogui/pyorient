from collections import namedtuple, OrderedDict
from ast import literal_eval
import re
from datetime import datetime
import sys

ScriptFunction = \
    namedtuple('Method', ['definition', 'signature', 'body', 'sha1'])

class Scripts(object):
    def __init__(self):
        self.namespaces = { 'default': {} }

    def add(self, functions, namespace=None):
        if namespace:
            dest = self.namespaces.get(namespace)
            if dest:
                dest.update(functions)
            else:
                self.namespaces[namespace] = functions
        else:
            self.namespaces['default'].update(functions)

    def get_scripts(self, namespace=None):
        if not namespace:
            namespace = 'default'

        return self.namespaces.get(namespace)

    def get_script(self, name, namespace=None):
        if not namespace:
            namespace = 'default'

        functions = self.namespaces.get(namespace)
        if functions:
            return functions.get(name)
        else:
            return None

    def script_body(self, name, args = None, namespace = None):
        function = self.get_script(name, namespace)
        if not function:
            return None

        param_string = re.search(r'\(([\w=\'", ]+)\)', function.signature)
        params = [ParamDefault(param.split('=')) for param in
                    param_string.group(1).split(',')] if param_string else None

        if isinstance(args, dict):
            param_defaults = \
                OrderedDict([val + (None,)
                    if len(val) == 1 else val for val in params])

            for param in params:
                if len(param) == 2 and param[0] not in args:
                    # Use default value
                    args[param[0]] = literal_eval(param[1])
            args = {k:v for k,v in args.items() if k in param_defaults.keys()}
        elif isinstance(args, tuple) or isinstance(args, list):
            args = { params[i][0]: args[i] if i < len(args)
                                           else literal_eval(params[i][1])
                        for i in range(0, max(len(params), len(args)))
                            if i < len(args) or len(params[i]) == 2 }
        else:
            if params:
                if args:
                    args = { params[0][0]: args }
                else:
                    if len(params[0]) == 2:
                        args = { params[0][0]: literal_eval(params[0][1]) }
                    else:
                        args = {}
            else:
                args = {}

        split_body = re.split(r'([\"\'])', function.body)

        replacements = {}
        for k, v in args.items():
            if isinstance(v, str) or isinstance(v, datetime):
                replacements[k] = "'{}'".format(v)
            elif sys.version_info[0] < 3 and isinstance(v, unicode):
                replacements[k] = repr(v.encode('utf-8'))
            else:
                replacements[k] = '{}'.format(v)

        for i, s in enumerate(split_body):
            if i % 4 == 0:
                for k, v in replacements.items():
                    split_body[i] = re.sub(r'\b{}\b'.format(k),
                                           v, split_body[i])

        return ''.join(split_body)

class ParamDefault(tuple):
    def __new__(self, pair):
        l = len(pair)
        if l > 2:
            raise ValueError('Only a name, and (optionally) a '
                'default value is valid for a parameter.')
        return tuple.__new__(self, (pair[0].strip(),) +
                                    ((pair[1],) if l > 1 else tuple()))

