from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

from datetime import datetime
import time;
  
KEYSPACE = "blockchaine"

cluster = Cluster()

print("Connecting to cluster...")
session = cluster.connect(KEYSPACE)


def block_insert(block_id, previous_block_id, time, hash_of_block):
    
    ## Insert in relation tables too
    query_block_insert = SimpleStatement("""INSERT INTO blocks (block_id, previous_block_id, time, hash_of_block)
                                VALUES  (%(id)s, %(old_id)s, %(t)s, %(h)s)
                                IF NOT EXISTS;
                                """, consistency_level=ConsistencyLevel.ONE)

    session.execute(query_block_insert, dict(id=block_id, old_id=previous_block_id, t=time, h=hash_of_block))


def transaction_insert(transaction_id, block_id, source_wallet, destination_wallet, num_coins, time, message_version):
    
    ## Insert in relation tables too
    query_transaction_insert = SimpleStatement("""INSERT INTO transactions (transaction_id, block_id, source_wallet, destination_wallet, num_coins, time, message_version)
                            VALUES (%(id)s, %(b_id)s, %(sw)s, %(dw)s, %(n)s, %(t)s, %(m)s)
                            IF NOT EXISTS;""", consistency_level=ConsistencyLevel.ONE) ## Insert in relation tables

    session.execute(query_transaction_insert, 
                    dict(id=transaction_id,
                        b_id=block_id,
                        sw=source_wallet,
                        dw=destination_wallet,
                        n=num_coins,
                        t=time,
                        m=message_version))

def pending_transaction_insert(transaction_id, time, status, transaction_object):

    ## Insert in relation tables too
    Pending_transaction_query = SimpleStatement("""INSERT INTO pending_transactions (transaction_id, time, status, transaction_object)
                                                VALUES (%(id)s, %(t)s, %(s)s, %(to)s)
                                                IF NOT EXISTS;""", consistency_level=ConsistencyLevel.ONE)

    session.execute(query_transaction_insert, 
                    dict(id=transaction_id,
                        t=time,
                        s=status,
                        to=transaction_object))