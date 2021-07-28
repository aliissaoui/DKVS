from cassandra.cluster import Cluster

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
def select_10min():
    condition = timestamp < current_timestamp - 10 min # ?
    query = "SELECT * FROM pending_transaction WHERE condition;"
    res = session.execute(query)
    return res

def select_prad(starting_from, block_count):
    query = "SELECT * FROM blocks WHERE time >= "+starting_from+" ORDER BY time LIMIT "+block_count+";"
    res = session.execute(query)
    return res

block_columns = select_blocks()
for col in block_columns:
    print(col)