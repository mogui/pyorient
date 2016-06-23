# The following code, derived from the bulbs project, carries this
# license:
"""
Copyright (c) 2012 James Thornton (http://jamesthornton.com)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

3. The name of the author may not be used to endorse or promote products
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import os
import io
import re
import string
import sre_parse
import sre_compile
from sre_constants import BRANCH, SUBPATTERN
import hashlib

from .scripts import ScriptFunction
from .utils import u

#
# The scanner code came from the TED project.
#

class Scanner(object):
    def __init__(self, lexicon, flags=0):
        self.lexicon = lexicon
        self.group_pattern = self._get_group_pattern(flags)

    def _get_group_pattern(self,flags):
        # combine phrases into a compound pattern
        patterns = []
        sub_pattern = sre_parse.Pattern()
        sub_pattern.flags = flags
        for phrase, action in self.lexicon:
            patterns.append(sre_parse.SubPattern(sub_pattern, [
                (SUBPATTERN, (len(patterns) + 1, sre_parse.parse(phrase, flags))),
                ]))
        #sub_pattern.groups = len(patterns) + 1
        group_pattern = sre_parse.SubPattern(sub_pattern, [(BRANCH, (None, patterns))])
        return sre_compile.compile(group_pattern)

    def get_multiline(self,f,m):
        content = []
        next_line = ''
        while not re.search("^}",next_line):
            content.append(next_line)
            try:
                next_line = next(f)
            except StopIteration:
                # This will happen at end of file
                next_line = None
                break
        content = "".join(content)
        return content, next_line

    def get_item(self,f,line):
        # IMPORTANT: Each item needs to be added sequentially
        # to make sure the record data is grouped properly
        # so make sure you add content by calling callback()
        # before doing any recursive calls
        match = self.group_pattern.scanner(line).match()
        if not match:
            return
        callback = self.lexicon[match.lastindex-1][1]
        if "def" in match.group():
            # this is a multi-line get
            first_line = match.group()
            body, current_line = self.get_multiline(f,match)
            sections = [first_line, body, current_line]
            content = "\n".join(sections).strip()
            callback(self,content)
            if current_line:
                self.get_item(f,current_line)
        else:
            callback(self,match.group(1))

    def scan(self, fin):
        for line in fin:
            self.get_item(fin,line)


class GroovyScripts(object):
    def __init__(self):
        self.functions = {}

    @classmethod
    def from_file(cls, groovy_path):
        parser = cls()
        parser.include(groovy_path)

        return parser.get_functions()

    @classmethod
    def from_string(cls, groovy_str):
        parser = cls()
        parser.parse(groovy_str)

        return parser.get_functions()

    def include(self, groovy_path):
        # handler format: (pattern, callback)
        handlers = [ ("^def( .*)", self.add_function), ]

        with io.open(groovy_path, 'r', encoding='utf-8') as groovy_file:
            Scanner(handlers).scan(groovy_file)

    def parse(self, groovy_str):
        handlers = [ ("^def( .*)", self.add_function), ]

        scanner = Scanner(handlers).scan(io.StringIO(u(groovy_str)))

    def get_functions(self):
        return self.functions

    # Scanner Callback
    def add_function(self,scanner,token):
        function_definition = token
        function_signature = self._get_function_signature(function_definition)
        function_name = self._get_function_name(function_signature)
        function_body = self._get_function_body(function_definition)
        # NOTE: Not using sha1, signature, or the full function right now
        # because of the way the GSE works. It's easier to handle version
        # control by just using the function_body, which the GSE compiles,
        # creates a class out of, and stores in a classMap for reuse.
        # You can't do imports inside Groovy functions so just using the func
        # body
        sha1 = self._get_sha1(function_definition)
        function = ScriptFunction(function_definition, function_signature
                                  , function_body, sha1)
        self.functions[function_name] = function

    def _get_function_signature(self,function_definition):
        pattern = '^def(.*){'
        return re.search(pattern,function_definition).group(1).strip()

    def _get_function_name(self,function_signature):
        pattern = '^(.*)\('
        return re.search(pattern,function_signature).group(1).strip()

    def _get_function_body(self,function_definition):
        # remove the first and last lines, and return just the function body
        lines = function_definition.split('\n')
        body_lines = lines[+1:-1]
        function_body = "\n".join(body_lines).strip()
        return function_body

    def _get_sha1(self,function_definition):
        # this is used to detect version changes
        function_definition_bytes = function_definition.encode('utf-8')
        sha1 = hashlib.sha1()
        sha1.update(function_definition_bytes)
        return sha1.hexdigest()

