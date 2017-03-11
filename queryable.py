import json
import time
import pytz
import traceback
import time_uuid
from pytz import timezone
from datetime import datetime
from pyspark.sql import types
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from config import *

conf = SparkConf()\
    .setAppName(APPNAME)\
    .setMaster(MASTER)\
    .set("spark.cassandra.connection.host", CASSANDRA_HOST)\
    .set("spark.cassandra.connection.port", CASSANDRA_PORT)\
    .set("spark.cassandra.auth.username", CASSANDRA_USERNAME)\
    .set("spark.cassandra.auth.password", CASSANDRA_PASSWORD)

sc = SparkContext(MASTER, APPNAME, conf=conf)
sqlContext = SQLContext(sc)
sqlContext.sql("""CREATE TEMPORARY TABLE %s \
                  USING org.apache.spark.sql.cassandra \
                  OPTIONS ( table "%s", \
                            keyspace "%s", \
                            cluster "Test Cluster", \
                            pushdown "true") \
              """ % (TABLE_QUERYABLE, TABLE_QUERYABLE, KEYSPACE))

start_time = time.time()

query = "SELECT event_name, COUNT(*) AS count FROM %s GROUP BY event_name ORDER BY count DESC" % (TABLE_QUERYABLE)
df_payload = sqlContext.sql(query)
df_payload.show()

print ("Queried in: %s seconds") % (time.time()-start_time)