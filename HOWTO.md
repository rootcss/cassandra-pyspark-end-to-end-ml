##Pyspark Cassandra

### Step by step guide to execute jobs

1. Install requirements
    - `pip install faker, cqlsh, time_uuid, pytz, cassandra-driver`

2. Create the file `config.py` by copying/renaming `config.py.sample`
    - Setup all the values accordingly.
    - Cassandra variables define which server to use.

3. Create the keyspace and all the tables.
    - Execute `schema_creator.py`
    - Note that, if you choose to drop the keypsace, all the data in that keyspace will be deleted.

4. Generating dummy data:
    - Configure `NUM_OF_RECORDS` in `config.py`. This tells the number of dummy records to be created.
    - Execute the Pyspark job `data_generator.py`
    - `spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.2 data_generator.py`
    - Wait for the job to be completed. It can be tracked in Spark UI.

5. Creating Data Models from above created events:
    - Execute the Pyspark job `modelling.py`
    - `spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.2 modelling.py`
    - Wait for the job to be completed.
    - More tables could be created as per the business requirements.

Now, the primary table and Data Model is ready. Check the Jupyter Notebook demo files to go through some queries.

1. Analysis and Machine Learning with Pyspark on Cassandra Data:
    - Check the `ml.ipynb` file for step by step instructions
2. Online/Offline ML models:
    - Check `ml-offline.ipynb` file - offline model
    - Check `ml-online.ipynb` file - online model
    - Offline model is run on schedule and the model file is extracted and passed on to be used by Online model

### How it works?
1. Spark generates dummy data and ingests that from Dataframe/RDD into a Cassandra table events_store.
2. Once the events_store is ready, Data models are created using a Spark Job. This processes the events_store data in batch, and ingest the data into data models.
3. Now, all these tables could be easily queried as shown in the demo files.
4. For machine learning and analysis, Pyspark fetches data from Cassandra and then processing happens on Spark Dataframes/RDD.


## Dummy Graph Data Tables
1. Go inside the folder `graph_data`
2. Rename `config.py.sample` as `config.py` and update the values accordingly.
3. Create the schema using python script: `python schema_creator.py`
4. Execute the Pyspark Job `data_generator.py`
    - `spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.2 data_generator.py`

## Demo Files
Following Jupyter Notebooks are available:
1. `cassandra_query_demo.ipynb`
2. `spark_query_demo.ipynb`
3. `graph_data/query_demo.ipynb`