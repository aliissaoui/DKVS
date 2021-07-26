from cassandra.cluster import Cluster

cluster = Cluster()

session = cluster.connect('test')

session.execute("INSERT INTO test_table (id, lastname, birthday) VALUES (uuid(), 'hehe', '2011-02-03 04:05+0000')")
result = session.execute("SELECT * FROM test_table")


### Transactions + blocks



def select_all(table):
    result = session.execute("SELECT * FROM "+table)
    return result

def insert(table, id, name, birthday):
    session.execute("INSERT INTO "+table+" (id, lastname, birthday) VALUES ("+id+", '"+name+"', '"+birdthay+"')")

def drop_table(table):
    session.execute("DROP TABLE "+table)

"""
def create_table(table_name, columns):
    query = "CREATE TABLE "+table_name+"( "
    for column in columns:
        query = """

print("result:\n", result[-1])
print("Done.")