from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

from datetime import datetime, timedelta
import time;

from select_data import select_blocks, select_by_timestamp, select_by_time

KEYSPACE = "blockchaine"

cluster = Cluster()

print("Connecting to cluster...")
session = cluster.connect(KEYSPACE)

def test_blocks():
    block_columns = select_blocks()
    for col in block_columns:
        print(col) 

def test_timestamp():
    r = select_by_timestamp('2021-07-28 15:41:12', 10)
    for e in r:
        print(e)

def main():
    res = test_blocks()
    print(res)

if __name__ == "__main__":
    main()