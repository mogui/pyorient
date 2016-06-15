import pyorient
# db_name = "GratefulDeadConcerts"
# client = pyorient.OrientDB("localhost", 2424)
# client.set_session_token(True)
# cluster_info = client.db_open( db_name, "admin", "admin" )
# print(client.db_count_records())

release = "2.2.0 (build develop@r79d281140b01c0bc3b566a46a64f1573cb359783; 2016-05-18 14:14:32+0000)"
x = pyorient.OrientVersion(release)
print(x.major)
print(x.minor)
print(x.build)
print(x.subversion)

release = "2.2.0-rc1"
x = pyorient.OrientVersion(release)
print(x.major)
print(x.minor)
print(x.build)
print(x.subversion)

release = "2.2.0-rc1"
x = pyorient.OrientVersion(release)
print(x.major)
print(x.minor)
print(x.build)
print(x.subversion)

release = "2.2.0 ;Unknown (build 0)"
x = pyorient.OrientVersion(release)
print(x.major)
print(x.minor)
print(x.build)
print(x.subversion)