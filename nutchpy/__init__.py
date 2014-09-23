from __future__ import absolute_import, division, print_function

"""Nutch python module"""

import logging

import os
import subprocess
import time
import py4j
from py4j.java_gateway import JavaGateway, GatewayClient, launch_gateway

from py4j.java_gateway import JavaGateway, GatewayClient, launch_gateway, java_import

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

cur_dir = os.path.dirname(__file__)
jar_dir = os.path.join(cur_dir,"java_libs")
jar_file = "seqreader-app-1.0-SNAPSHOT-jar-with-dependencies.jar"
jar_full = os.path.join(jar_dir,jar_file)

main_class = "com.continuumio.seqreaderapp.App"

port = int(os.getenv('NUTCHPY_GATEWAY_PORT',0))

cmd_dict = {"jar_full": jar_full, "main_class": main_class, 'port': port}

java_cmd = "/usr/bin/java -cp ::{jar_full} -Xms512m -Xmx512m {main_class} {port}".format(**cmd_dict)
ps = subprocess.Popen(java_cmd, shell=os.name != 'nt',
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# wait for JVM to start
time.sleep(1)
logger.debug(java_cmd)
port = int(ps.stdout.readline())


gateway = JavaGateway(GatewayClient(port=port, auto_close=True),auto_convert=True)
logger.info("JAVA GATEWAY STARTED ON PORT: %d"% (port,) )

seq_reader = gateway.entry_point
