from cassandra.cluster import Cluster

KEYSPACE = "blockchaine"

cluster = Cluster()

print("Connecting to cluster...")
session = cluster.connect(KEYSPACE)


###### "tables"
Block_query =   """ CREATE TABLE IF NOT EXISTS blocks( 
                    block_id INT PRIMARY KEY,
                    previous_block_id INT,
                    time TIMESTAMP,
                    hash_of_block TEXT,
                    );"""
                    

Transaction_query = """CREATE TABLE IF NOT EXISTS transactions( 
                    transaction_id INT PRIMARY KEY,
                    block_id INT,
                    source_wallet TEXT,
                    destination_wallet TEXT,
                    Num_coins INT,
                    time TIMESTAMP,
                    message_version TEXT,
                    );"""

Pending_transaction_query = """CREATE TABLE IF NOT EXISTS pending_transactions( 
                    transaction_id INT PRIMARY KEY,
                    time TIMESTAMP,
                    status TEXT,
                    transaction_object BLOB,
                    );"""

print("Creating block table...")
session.execute(Block_query)

print("Creating transaction table...")
session.execute(Transaction_query)

print("Creating pending transaction table...")
session.execute(Pending_transaction_query)

###### "Relations tables"
Block_transaction_query = """CREATE TABLE IF NOT EXISTS transactions_by_block( 
                            transaction_id INT PRIMARY KEY,
                            block_id INT,
                            Num_coins INT,
                            time TIMESTAMP,
                            );"""

Wallet_query = """CREATE TABLE IF NOT EXISTS wallets( 
                            wallet_id INT PRIMARY KEY,
                            transaction_id INT,
                            block_id INT,
                            time TIMESTAMP,
                            );"""

print("Creating transactions by block table...")
session.execute(Block_transaction_query)

print("Creating wallets table...")
session.execute(Wallet_query)
