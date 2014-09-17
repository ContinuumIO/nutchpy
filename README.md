
## Install
- https://github.com/ContinuumIO/nutchpy.git
- conda install -c blaze apache-maven
- cd seqreader-app/
- mvn package
 - create single jar with dependencies
 - `java -jar target/seqreader-app-1.0-SNAPSHOT-jar-with-dependencies.jar`

## Running

```python
from py4j.java_gateway import JavaGateway
gateway = JavaGateway()

seq_reader = gateway.entry_point
node_path = "FULL-PATH/nodes/part-00000/data"
print(seq_reader.head(10,node_path))
print(seq_reader.slice(10,20,node_path))
```

## Requirements

- JDK 1.6+
- python
- py4j
