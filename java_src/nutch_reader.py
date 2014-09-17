from __future__ import absolute_import, division, print_function

import os
import subprocess
from py4j.java_gateway import JavaGateway

jar_dir = os.path.join(os.path.abspath('../'),"seqreader-app","target")
jar_file = "seqreader-app-1.0-SNAPSHOT-jar-with-dependencies.jar"
jar_full = os.path.join(jar_dir,jar_file)

main_class = "com.continuumio.seqreaderapp.App"

cmd_dict = {"jar_full": jar_full, "main_class": main_class}

java_cmd = 'exec java -cp ::{jar_full} {main_class} -XX:MaxPermSize=128m -Xms512m "$@" '.format(**cmd_dict)
ps = subprocess.Popen(java_cmd, shell=os.name != 'nt',
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)



gateway = JavaGateway(eager_load=True)
gateway.jvm.System.getProperty("java.runtime.name")

seq_reader = gateway.entry_point
node_path = "<FULL-PATH>/nodes/part-00000/data"
node_path = "/Users/quasiben/Research/ContinuumDev/Memex/nutchpy/java_src/segments/20140916105221/content/part-00000/data"
print(seq_reader.head(10,node_path))
print(seq_reader.slice(10,20,node_path))

