from cassandra.cluster import Cluster

KEYSPACE = "blockchaine"

cluster = Cluster()

print("Connecting to cluster...")
session = cluster.connect(KEYSPACE)

def block_select():
    query = "SELECT * FROM blocks"
    res = session.execute(query)
    return res


def transaction_select():
    query = "SELECT * FROM transactions"
    res = session.execute(query)
    return res


def block_transaction_select():
    query = "SELECT * FROM transactions_by_block"
    res = session.execute(query)
    return res


block_columns = block_select()
for col in block_columns:
    print(col)