pyorient: OrientDB native client library
========================================

.. image:: https://img.shields.io/pypi/v/pyorient.svg
    :target: https://pypi.python.org/pypi/pyorient

.. image:: https://img.shields.io/pypi/dm/pyorient.svg
        :target: https://pypi.python.org/pypi/pyorient

.. image:: https://travis-ci.org/mogui/pyorient.svg?branch=master
    :target: https://travis-ci.org/mogui/pyorient

.. image:: https://coveralls.io/repos/mogui/pyorient/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/mogui/pyorient?branch=master

`Orientdb <http://www.orientechnologies.com>`_ driver for python that uses the binary protocol.

Pyorient works with orientdb version 1.7 and later.

**Warning** Some issues are experimented with record_create/record_upload and OrientDB < 2.0. These command are strongly discouraged with these versions

**NOTICE** Prior to version 1.4.7 there was a potential SQL injection vulnerability that now is fixed. (see `details <https://github.com/mogui/pyorient/pull/172>`_ )

Installation
************
::

  pip install pyorient


How to contribute
*****************

- Fork the project
- work on **develop** branch
- Make your changes
- Add tests for it. This is important so I don't break it in a future version unintentionally
- Send me a pull request *(pull request to master will be rejected)*
- ???
- PROFIT
