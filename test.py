import pyorient
db_name = "GratefulDeadConcerts"
client = pyorient.OrientDB("localhost", 2424)
client.set_session_token(True)
cluster_info = client.db_open( db_name, "admin", "admin" )
print(client.db_count_records())
