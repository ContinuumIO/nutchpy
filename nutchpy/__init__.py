from __future__ import absolute_import, division, print_function

"""Nutch python module"""

import logging

import os
import subprocess
import time
import py4j
from py4j.java_gateway import JavaGateway, GatewayClient, launch_gateway

cur_dir = os.path.dirname(__file__)
jar_dir = os.path.join(cur_dir,"java_libs")
jar_file = "seqreader-app-1.0-SNAPSHOT-jar-with-dependencies.jar"
jar_full = os.path.join(jar_dir,jar_file)

main_class = "com.continuumio.seqreaderapp.App"

cmd_dict = {"jar_full": jar_full, "main_class": main_class}


# add better JVM initialization
java_cmd = "java -jar {jar_full} {main_class}".format(**cmd_dict)
ps = subprocess.Popen(java_cmd, shell=os.name != 'nt',
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)


# import pdb
# pdb.set_trace()
time.sleep(1)
# wait for JVM to start

gateway = JavaGateway(GatewayClient(port=25333, auto_close=True),)
print(gateway.jvm.System.getProperty("java.runtime.name"))

seq_reader = gateway.entry_point

import atexit

def exit_handler():
    ps.kill()

atexit.register(exit_handler)

#
# port = launch_gateway(jarpath=path,classpath="com.continuumio.seqreaderapp.App",
#                       javaopts=["-XX:MaxPermSize=128m", "-Xms512m", "-Xmx512m"])
#
# port = launch_gateway(jarpath=path, javaopts=["com.continuumio.seqreaderapp.App",
#                       "-Xms512m", "-Xmx512m"])
