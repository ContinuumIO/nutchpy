# Nutchpy

## Introduction

Nutchpy is a Python library for working with [Apache Nutch](http://nutch.apache.org).
In particular, the library provides functionality to work with existing Nutch data structures including various eeaders for the Nutch EcoSystem e.g. readers for [Sequence Files](http://wiki.apache.org/hadoop/SequenceFile), [LinkDb](http://nutch.apache.org/apidocs/apidocs-1.9/index.html?org/apache/nutch/crawl/LinkDb.html), [Nodes](http://nutch.apache.org/apidocs/apidocs-1.9/index.html?org/apache/nutch/scoring/webgraph/Node.html), etc.
A small [examples](/examples) directory exists showing how Nutchpy can be used to interact with some of the above data strutures. 

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
