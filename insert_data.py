from cassandra.cluster import Cluster

KEYSPACE = "blockchaine"

cluster = Cluster()

print("Connecting to cluster...")
session = cluster.connect(KEYSPACE)

query_block_insert = """ INSERT INTO blocks (block_id, previous_block_id, time, hash_of_block)
                            VALUES (1, 0, '2021-07-26', '100')
                            IF NOT EXISTS;"""


query_transaction_insert = """ INSERT INTO transactions (transaction_id, block_id, source_wallet, destination_wallet, num_coins, time, message_version)
                            VALUES (1, 0, 'Ali', 'Jack', 10, '2021-07-26', 'new')
                            IF NOT EXISTS;"""

print("Inserting row in blocks...")
session.execute(query_block_insert)

print("Inserting row in transactions...")
session.execute(query_transaction_insert)

def block_insert(block_id, previous_block_id, time, hash_of_block):
    query_block_insert = """ INSERT INTO blocks (block_id, previous_block_id, time, hash_of_block)
                            VALUES (%x, %x, %s, %s)
                            IF NOT EXISTS; """ % (block_id, previous_block_id, time, hash_of_block)
    ## Insert in relation tables too
    session.execute(query_block_insert)
    

def transaction_insert(transaction_id, block_id, source_wallet, destination_wallet, num_coins, time, message_version):
    query_transaction_insert = """ INSERT INTO transactions (transaction_id, block_id, source_wallet, destination_wallet, num_coins, time, message_version)
                                VALUES (%x, %x, %s, %s, %x, %s, %s)
                                IF NOT EXISTS;""" % (transaction_id, block_id, source_wallet, destination_wallet, num_coins, time, message_version)
    ## Insert in relation tables
    session.execute(query_block_insert)
