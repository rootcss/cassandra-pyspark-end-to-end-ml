## Spark-Cassandra

<br>
1. `data_generator.py`: Spark Job to create fake data and store into Cassandra
<br>
2. `data_faker.py`: Designs the payload for fake data
<br>
3. `modelling.py`: Creates Data Models from primary table of JSON data
<br>
4. `queryable.py`: Allows to write SQL query on Data Models, using Spark as backend.
<br>
5. `config.py.sample`: Copy the file to `config.py` and set values.
<br>
<br>
## Execution
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.2 data_generator.py
```
<br>
<br>

## Setup of Cassandra's Schema
```sql
CREATE KEYSPACE shekhar_upwork WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
```

```sql
CREATE TABLE events_store (
    bucket_id     text,
    event_id      timeuuid,
    event_name    text,
    payload       text,
    PRIMARY KEY (bucket_id, event_id)
) WITH CLUSTERING ORDER BY (event_id DESC);
```

```sql
CREATE TABLE queryable_users (
    bucket_id       text,
    unix_timestamp  bigint,
    event_id        text,
    event_name      text,
    name            text,
    city            text,
    zipcode         text,
    created_at      timestamp,
    email           text,
    job             text,
    gender          text,
    age             int,
    PRIMARY KEY (bucket_id, unix_timestamp)
) WITH CLUSTERING ORDER BY (unix_timestamp DESC);

CREATE INDEX ON shekhar_upwork.queryable_users (email);
```

## Install requirements
```
pip install faker, cqlsh, time_uuid, pytz
```