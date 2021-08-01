from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from datetime import datetime, timedelta

KEYSPACE = "blockchaine"

cluster = Cluster()

print("Connecting to cluster...")
session = cluster.connect(KEYSPACE)

def select_blocks():
    query = "SELECT * FROM blocks"
    res = session.execute(query)
    return res

def select_transaction():
    query = "SELECT * FROM transactions"
    res = session.execute(query)
    return res

def select_block_transaction():
    query = "SELECT * FROM transactions_by_block"
    res = session.execute(query)
    return res

# For Nupur
def select_by_time(delta):

    time_condition = datetime.now() - timedelta(minutes=delta)
    query = SimpleStatement("SELECT * FROM transactions WHERE time < %(s)s ALLOW FILTERING;",
                             consistency_level=ConsistencyLevel.ONE)
    res = session.execute(query, dict(s=time_condition))

    return res

# For prad
def select_by_timestamp(starting_from, block_count):
    query = SimpleStatement("""SELECT * FROM blocks
                                WHERE time >= %(s)s
                                LIMIT %(b)s
                                ALLOW FILTERING """, consistency_level=ConsistencyLevel.ONE)
    res = session.execute(query, dict(s=starting_from, b=block_count))
    return res

