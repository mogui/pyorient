import pyorient
db_name = "GratefulDeadConcerts"
client = pyorient.OrientDB("localhost", 2424)
cluster_info = client.db_open( db_name, "admin", "admin" )
print(cluster_info)

cluster_info2 = client.db_open( db_name, "admin", "admin" )
print(cluster_info2)