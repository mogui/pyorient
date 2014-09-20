# pyorient

[Orientdb](chcadas161.emea.guccigroup.dom/IntMailTrack) driver for python that uses the binary protocol.

Pyorient works with orientdb version 1.7 and later.


### Installation

	pip install pyorient


## Testing

To run the tests you need `nose`

	pip install nose
	
then you can run tests with:

	nosetests 


## Usage

For full range of commands refer to this page: [API](https://github.com/mogui/pyorient/wiki/API) or read the tests!

### init the client

	client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect( "admin", "admin" )
    
### Add a cluster

	new_cluster_id = client.data_cluster_add('my_cluster_1234567',   
	    pyorient.CLUSTER_TYPE_PHYSICAL )
	    
### Open a DB
	client.db_open(db_name, "admin", "admin",   
		pyorient.DB_TYPE_GRAPH, "" )

### Make a query
	result = client.query("select from my_class", 10, '*:0')
	
### Execute a command
	cluster_id = client.command( "create class my_class extends V" )


## Contributions

- Fork the project.
- Make your changes.
- Add tests for it. This is important so I don’t break it in a future version unintentionally.
- Send me a pull request.
- ???
- PROFIT

## Authors
- [mogui](https://github.com/mogui/)
- [ostico](https://github.com/ostico/)

## Copyright

Copyright © 2012 Niko Usai. See LICENSE for details.
