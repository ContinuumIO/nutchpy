import os
import nutchpy

seq_reader = nutchpy.seq_reader
path = os.path.dirname(nutchpy.__file__)
path = os.path.join(path,"ex_data", "crawldb_data")

print(nutchpy.seq_reader.head(5,path))
print(seq_reader.slice(5,10,path))
