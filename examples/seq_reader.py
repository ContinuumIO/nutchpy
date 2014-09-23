import os
import nutchpy

seq_reader = nutchpy.seq_reader
path = os.path.dirname(nutchpy.__file__)
path = os.path.join(path,"ex_data", "crawldb_data")

data = seq_reader.head(5,path)
print(data)
print(len(data))

data = seq_reader.slice(5,10,path)
print(data)
print(len(data))

#hadoop fs -text path <-- equivalent
data = seq_reader.read(path)
print(data)
print(len(data))
print(path)
