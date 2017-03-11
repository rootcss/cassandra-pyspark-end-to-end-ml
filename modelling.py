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
              """ % (TABLE_EVENT_STORE, TABLE_EVENT_STORE, KEYSPACE))

start_time = time.time()

queryableModel = StructType([ \
        StructField("bucket_id",       StringType(),     True), \
        StructField("unix_timestamp",  LongType(),       True), \
        StructField("event_id",        StringType(),     True), \
        StructField("event_name",      StringType(),     True), \
        StructField("name",            StringType(),     True), \
        StructField("city",            StringType(),     True), \
        StructField("zipcode",         StringType(),     True), \
        StructField("created_at",      StringType(),     True), \
        StructField("email",           StringType(),     True), \
        StructField("job",             StringType(),     True), \
        StructField("gender",          StringType(),     True), \
        StructField("age",             IntegerType(),    True), \
      ])

def get_unix_timestamp_from_timeuuid(timeuuid):
    return int(time_uuid.TimeUUID(timeuuid).get_timestamp()*100000000)

def payloadParser(event_data):
    event_id = event_data[0]
    event_name = event_data[1]
    bucket_id = event_data[2]
    unix_timestamp = get_unix_timestamp_from_timeuuid(event_id)
    payload = json.loads(event_data[3])
    try:
      p = ( bucket_id,\
            unix_timestamp,\
            event_id,\
            event_name,\
            payload.get('name', None),\
            payload.get('city', None),\
            payload.get('zipcode', None),\
            payload.get('created_at', None),\
            payload.get('email', None),\
            payload.get('job', None),\
            payload.get('gender', None),\
            payload.get('age', None)
          )
    except Exception as e:
      print e
      print '................................'
      p = ('NA', 'NA', 'NA', 'NA', 'NA', 'NA',\
           'NA', 'NA', 'NA', 'NA', 'NA', 'NA',)
    return p

query = "SELECT CAST(event_id AS string), event_name, bucket_id, payload FROM " + TABLE_EVENT_STORE
df_payload = sqlContext.sql(query)
parsed_data = df_payload.map(lambda bbb: payloadParser(bbb))
output_df = sqlContext.createDataFrame(parsed_data, schema=queryableModel)
output_df.write.format("org.apache.spark.sql.cassandra")\
               .options(table=TABLE_QUERYABLE, keyspace=KEYSPACE)\
               .save(mode='append')

print ("Ingested in: %s seconds") % (time.time()-start_time)