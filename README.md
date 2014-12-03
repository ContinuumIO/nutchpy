
## Install

To build `nutchpy` from source, run the following commands in your terminal:

```bash
  git clone https://github.com/ContinuumIO/nutchpy.git
  conda install -c blaze apache-maven
  cd nutchpy; python setup.py install;
```

Alternatively, you can download `nutchpy` from [binstar](https://binstar.org/) with [conda](http://conda.pydata.org/):

```bash
  conda install -c blaze nutchpy
```

## Running

```python
import nutchpy

node_path = "<FULL-PATH>/data"
seq_reader = nutchpy.seq_reader
print(seq_reader.head(10,node_path))
print(seq_reader.slice(10,20,node_path))
```

## Run Requirements

- JDK 1.6+
- python
- py4j

## Build Requirements

- python
- apache-maven (`conda install -c blaze apache-maven`)
