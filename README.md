# pyorient v0.1

pyorient is a wrapper over the c library [**liborient**](https://github.com/dam2k/liborient) which is a client for the Binary Protocol of the **NoSQL Graph-Document DBMS** [**OrientDB**](http://www.orientdb.org/)

This software is in development stage. Do not use it in production environments if you do not know what you are doing.


## Installation

### Prerequisites

- Python 2.6.5 or higher (tested from this, could work with previous version) 
- liborient [by dam2k](https://github.com/dam2k/liborient)

### Building and Installing
	
- `tar pyorient-*.tar.gz`
- `cd pyorient-*`
- [optionally] edit config.cfg if you've installed liborient in a non standard path 
- `python setup.py build`
- `sudo python setup.py install`

#### Linux

If you install liborient to a location other than the default (/usr/local) on Linux, you may need to do one of the following:

- Set `LD_LIBRARY_PATH` to point to the `lib` directory of liborient.
- Build the extension using the `--rpath` flag:   
`python setup.py build_ext --rpath=/opt/lib`


once a bit more stable I plan to distribute the lib via pip

## Testing

To run the tests suite after installing (or before installing, but building with `python setup.py build_ext --inplace` flag)   
change the file `tests.cfg` with valid credential of a running OrientDB instance, and then do:

	python setup.py test

## Known Issues and limitation

- ORecordCoder cannot parse nested list (on schedule to be implemented)
- method *command* doesn't discern malformed query from empty query result it always returns an empty query set (it must throw an exception), will be fixed soon (it's a c lib issue)
- OrientRecord  created by *command* and *recordload* methods doesn't properly handle RID and VERSION field due to a limitation in the c library (soon to be fixed)
- few methods are missing like **recorddelete** and **recordupdate** (they aren't present in the c lib also theese one soon to be covered)

## Getting started

Refer to the [wiki pages](http://asd.it) and tests for the methods exposed by the module.

#### Imports

	import pyorient

#### Sample session
	# open connection
	host = 'localhost'
	port = '2424'
	user = 'admin'
	password = 'admin'
	db = pyorient.OrientDB(host, port, user, password)

	# open a database
	db.dbopen('demo')

	# create a record in a cluster of the database
	record = pyorient.OrientRecord({
	    'context':'test pyorient',
	    'quantity': 23,
	    'date' : datetime.datetime.now()
	}, o_class='Order')
	cluster_id = 55

	# retrieve record
	position = db.recordcreate(cluster_id, record)
	retrieved_record = db.recordload(cluster_id, position)

	print retrieved_record.context
	# closes the connection
	db.close()

## Note on Patches/Pull Requests

- Fork the project.
- Make your changes.
- Add tests for it. This is important so I don’t break it in a future version unintentionally.
- Commit
- Send me a pull request.
- ???
- PROFIT

## Copyright

Copyright © 2012 Niko Usai. See LICENSE for details.
