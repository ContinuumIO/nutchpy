import os
import nutchpy

path = os.path.dirname(nutchpy.__file__)
path = os.path.join(path,"ex_data", "crawldb_data")

data = nutchpy.sequence_reader.head(5,path)
# print(data)
assert len(data) == 5

data = nutchpy.sequence_reader.slice(5,20,path)
# print(data)
assert len(data) == 3

#hadoop fs -text path <-- equivalent
data = nutchpy.sequence_reader.read(path)
# print(data)
assert len(data) == 8


count = nutchpy.sequence_reader.count(path)
assert count == 8

docs = nutchpy.sequence_reader.buffered_read(path)
count2 = 0
for doc in docs:
    count2 += 1

assert count2 == count

limit = 3
docs = nutchpy.sequence_reader.read_all([path], limit=limit)
for doc in docs:
    limit -= 1
assert limit == 0
