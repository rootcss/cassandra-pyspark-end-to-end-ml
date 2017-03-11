import json
import time
from data_faker import event
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
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

start_time = time.time()

events = sc.parallelize(xrange(0, NUM_OF_RECORDS))\
           .map(lambda x: event())\
           .map(lambda x: json.loads(x))\
           .toDF()

events.write.format("org.apache.spark.sql.cassandra")\
            .options(table=TABLE_EVENT_STORE, keyspace=KEYSPACE)\
            .save(mode='append')

print ("%s rows ingested in: %s seconds") % (NUM_OF_RECORDS, time.time()-start_time)