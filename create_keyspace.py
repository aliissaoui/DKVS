from cassandra.cluster import Cluster

KEYSPACE = "blockchaine"
REP_FAC = 1

cluster = Cluster()

print("Connecting to cluster...")

session = cluster.connect()


print("Creating keyspace...")

session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '%x' }
        """ % (KEYSPACE, REP_FAC))

description = session.execute("DESCRIBE keyspaces;")

#for keyspace in description:
#       print(keyspace)

print("Setting keyspace...")

session.set_keyspace(KEYSPACE)

