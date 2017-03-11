from config import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import sys

def create_events_store_table():
    cmd = "CREATE TABLE %s (\
                bucket_id     text,\
                event_id      timeuuid,\
                event_name    text,\
                payload       text,\
                PRIMARY KEY (bucket_id, event_id)\
            ) WITH CLUSTERING ORDER BY (event_id DESC)" % (TABLE_EVENT_STORE)
    print "Creating table %s" % (TABLE_EVENT_STORE)
    session.execute(cmd)

def create_queryable_table():
    cmd = "CREATE TABLE %s (\
                bucket_id       text,\
                unix_timestamp  bigint,\
                event_id        text,\
                event_name      text,\
                name            text,\
                city            text,\
                zipcode         text,\
                created_at      timestamp,\
                email           text,\
                job             text,\
                gender          text,\
                age             int,\
                PRIMARY KEY (bucket_id, unix_timestamp)\
            ) WITH CLUSTERING ORDER BY (unix_timestamp DESC)" % (TABLE_QUERYABLE)
    print "Creating table %s" % (TABLE_QUERYABLE)
    session.execute(cmd)
    print "Creating an index. Could be added more when needed. Max 5"
    cmd = "CREATE INDEX ON %s (email)" % (TABLE_QUERYABLE);
    print "Creating index %s.email" % (TABLE_QUERYABLE)
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
create_events_store_table()
create_queryable_table()
print ""
print ""
print "Done."