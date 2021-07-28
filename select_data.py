from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import time

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

# INCOMPLETE
"""def select_10min():
    condition = timestamp < current_timestamp - 10 min # ?
    query = "SELECT * FROM pending_transaction WHERE condition;"
    res = session.execute(query)
    return res"""

def select_by_timestamp(starting_from, block_count):
    query = SimpleStatement(""" SELECT * FROM blocks
                                WHERE time >= %(s)s
                                LIMIT %(b)s
                                ALLOW FILTERING """, consistency_level=ConsistencyLevel.ONE)
    res = session.execute(query, dict(s=starting_from, b=block_count))
    return res

