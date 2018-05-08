# kafka-connect-python
Python module for Kafka Connect REST API


## Requirements
* Python (3.6)

## Installation

Install using `pip`...

```sh
    pip install kafka-connect-python
```

## Examples

### Create KafkaConnect REST Interface
```python
from kafka_connect import KafkaConnect

connect = KafkaConnect(host='localhost', port=8083, scheme='http')

print(connect.api.version)

```

### Create a connector using config dictionary
```python
config = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://localhost/testdb?user=testuser&password=SecurePassword!",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "table.whitelist": "sampletable",
    "mode": "timestamp",
    "timestamp.column.name": "lastupdated",
    "topic.prefix": "test-0-"
}

connect.connectors['sample-connector'] = config
```

### Update connector config
```pyton
connector = connect.connectors['sample-connector']
connector.config['poll.time.ms'] = 500
```

### List connector names
```python
list(map(lambda c: c.name, connect.connectors))
```

### Iterate over connectors and tasks
```python
for connector in connect.connectors:
    for task in connector.tasks:
        task.restart()
```

### Delete a connector
```python
del connect.connectors['sample-connector']
```
