import os
import nutchpy

path = os.path.dirname(nutchpy.__file__)
path = os.path.join(path,"ex_data", "crawldb_data")

data = nutchpy.sequence_reader.head(5,path)
# print(data)
assert len(data) == 5

data = nutchpy.sequence_reader.slice(5,20,path)
# print(data)
assert len(data) == 2

#hadoop fs -text path <-- equivalent
data = nutchpy.sequence_reader.read(path)
# print(data)
assert len(data) == 8



