from __future__ import absolute_import, division, print_function


"""
JVM Gateway
"""


import os
import sys
import subprocess
import time
import logging
import py4j
from threading import Thread

from py4j.java_gateway import JavaGateway, GatewayClient, launch_gateway, java_import

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def launch_gateway():

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

    gateway = JavaGateway(GatewayClient(port=port),auto_convert=True)
    logger.info("JAVA GATEWAY STARTED ON PORT: %d"% (port,) )

    java_import(gateway.jvm, 'com.continuumio.seqreaderapp.LinkReader')
    java_import(gateway.jvm, 'com.continuumio.seqreaderapp.NodeReader')
    java_import(gateway.jvm, 'com.continuumio.seqreaderapp.SequenceReader')

    ## STOLEN SHAMELESS FROM APACHE/SPARK
    # Create a thread to echo output from the GatewayServer, which is required
    # for Java log output to show up:
    class EchoOutputThread(Thread):

        def __init__(self, stream):
            Thread.__init__(self)
            self.daemon = True
            self.stream = stream

        def run(self):
            while True:
                line = self.stream.readline()
                sys.stderr.write(line)
    EchoOutputThread(ps.stdout).start()

    return gateway

class Singleton:
    def __init__(self,klass):
        self.klass = klass
        self.instance = None
    def __call__(self,*args,**kwds):
        if self.instance == None:
            self.instance = self.klass(*args,**kwds)
        return self.instance


@Singleton
class NutchJavaGateway:
  _gateway = None

  @property
  def gateway(self):
    if self._gateway is None:
      self._gateway = launch_gateway()
    return self._gateway

  def __del__(self):
      if self._gateway:
          logger.info("SHUTTING DOWN JAVA GATEWAY ")
          self._gateway.shutdown(raise_exception=True)
          logger.debug("SHUTDOWN COMPLETE")

gateway = NutchJavaGateway().gateway
