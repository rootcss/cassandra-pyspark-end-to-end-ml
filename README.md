## Spark-Cassandra

* `data_generator.py`: Spark Job to create fake data and store into Cassandra
* `data_faker.py`: Designs the payload for fake data
* `modelling.py`: Creates Data Models from primary table of JSON data
* `queryable.py`: Allows to write SQL query on Data Models, using Spark as backend.
* `config.py.sample`: Copy the file to `config.py` and set values.
* `schema_creator.py`: Creates the required schema in Cassandra.

Check HOWTO file for all instructions.