
from py4j.java_gateway import JavaGateway
gateway = JavaGateway()

seq_reader = gateway.entry_point
node_path = "<FULL-PATH>/nodes/part-00000/data"
print(seq_reader.head(10,node_path))
print(seq_reader.slice(10,20,node_path))

