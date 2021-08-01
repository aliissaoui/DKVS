from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

from datetime import datetime, timedelta
import time;

from insert_data import block_insert, transaction_insert

def insert_10_blocks():
    for i in range(2, 12):
        block_insert(i, i-1, datetime.utcnow(), "0000"+str(i))
        #time.sleep(1)

def insert_10_transactions():
    for i in range(2, 12):
        transaction_insert(i, 1, "100"+str(i), "101"+str(i), i+10, datetime.utcnow(), "TEXT")
        #time.sleep(1)

def main(): 
    KEYSPACE = "blockchaine"

    cluster = Cluster()

    print("Connecting to cluster...")
    session = cluster.connect(KEYSPACE)

    timestamp = int(time.time())
    now = datetime.utcnow()

    print("Inserting 10 blocks...")
    insert_10_blocks()

    print("Inserting 10 transactions...")
    insert_10_transactions()


    print(now)
    print(now-timedelta(days=1))

if __name__ == "__main__":
    main()