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
    
### Create a DB
        client.db_create( db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY )

### Check if a DB exists
	client.db_exists( db_name, pyorient.STORAGE_TYPE_MEMORY )

### Open a DB
	client.db_open( db_name, "admin", "admin" )

### Send a command
	cluster_id = client.command( "create class my_class extends V" )
	client.command( 
	  "insert into my_class ( 'accommodation', 'work', 'holiday' ) values( 'B&B', 'garage', 'mountain' )"
	)

### Create a record
	rec = { '@my_class': { 'accommodation': 'house', 'work': 'office', 'holiday': 'sea' } }
	rec_position = client.record_create( cluster_id, rec )

### Load a record
	client.record_load( rec_position.rid )

### Make a query
	result = client.query("select from my_class", 10, '*:0')

### Delete a record
	client.record_delete( cluster_id, rec_position.rid )

### Drop a DB
	client.db_drop( db_name )


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
