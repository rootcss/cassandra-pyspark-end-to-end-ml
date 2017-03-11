##Pyspark Cassandra

### Step by step guide to execute jobs

1. Create the file `config.py` by copying config.py.sample
    - Setup all the values accordingly.
    - Cassandra variables defines which server to use.

2. Create the keyspace and all the tables.
    - Execute `schema_creator.py`
    - Note that, if you choose to drop the keypsace, all the data in that keyspace will be deleted.

3. Generating dummy data:
    - Configure `NUM_OF_RECORDS` in `config.py`. This tells the number of records to be created.
    - Execute the Pyspark job `data_generator.py`
    - `spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.2 data_generator.py`
    - Wait for the job to be completed. It can be tracked in Spark UI.

4. Creating Data Models from above created events:
    - Execute the Pyspark job `modelling.py`
    - `spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.2 modelling.py`
    - Wait for the job to be completed.
    - More tables could be created as per the business requirements.

Now, the primary table and Data Model is ready.
- Check the Jupyter Notebook file demo files to go through some queries.

### How it works?
1. Spark generates dummy data and ingests that from Dataframe/RDD into a Cassandra table events_store.
2. Once the events_store is ready, Data models are created using a Spark Job. This process the events_store data in batch, and fill up data into data models.
3. Now, all these tables could be easily queried as shown in demo file.