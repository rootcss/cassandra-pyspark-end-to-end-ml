from config import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import sys

def create_edges_table():
    cmd = "CREATE TABLE %s (\
                comment_mobile_orig     bigint,\
                updated_at              timestamp,\
                comment_duration        int,\
                comment_type            text,\
                contact_name            text,\
                src                     text,\
                dst                     text,\
                tagged_contact_name     text,\
                PRIMARY KEY (comment_mobile_orig, updated_at)\
            ) WITH CLUSTERING ORDER BY (updated_at DESC)" % (TABLE_EDGES)
    print "Creating table %s" % (TABLE_EDGES)
    session.execute(cmd)

def create_vertices_table():
    cmd = "CREATE TABLE %s (\
                id                  text,\
                mobilenumber        bigint,\
                PRIMARY KEY (id, mobilenumber)\
            ) WITH CLUSTERING ORDER BY (mobilenumber DESC)" % (TABLE_VERTICES)
    print "Creating table %s" % (TABLE_VERTICES)
    session.execute(cmd)

def handle_keyspace_operations():
    rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        print "Keyspace %s already exists. Dropping it.." % (KEYSPACE)
        if raw_input('Are you sure? (Y/n) Deleting a keyspace will DELETE all data in it. > ')[0] == 'Y':
            cmd = "DROP KEYSPACE %s" % (KEYSPACE)
            print "Dropping keyspace %s" % (KEYSPACE)
            session.execute(cmd)
            create_keyspace()
        else:
            print 'Exiting Now! Maybe you can create with a different keyspace name.'
            sys.exit(0)
    else:
        print "Keyspace %s doesn't exist, will create it." % (KEYSPACE)
        create_keyspace()

def create_keyspace():
    cmd = "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" % KEYSPACE
    print "Creating keyspace %s" % (KEYSPACE)
    session.execute(cmd)
    print "Further queries will be executed against keyspace: %s" % (KEYSPACE)
    session.set_keyspace(KEYSPACE)

auth_provider = PlainTextAuthProvider(
                  username=CASSANDRA_USERNAME,
                  password=CASSANDRA_PASSWORD)

cluster = Cluster(
            contact_points = [CASSANDRA_HOST],
            port = CASSANDRA_PORT,
            auth_provider = auth_provider)

global session
session = cluster.connect()
print "Loading values from config.py"
handle_keyspace_operations()
create_edges_table()
create_vertices_table()
print ""
print ""
print "Done."