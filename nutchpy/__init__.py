"""Nutch python module"""

from py4j.java_gateway import java_import, JavaGateway, GatewayClient

# launch_gateway(port=0, jarpath=u'', classpath=u'', javaopts=, []die_on_exit=False)
gateway = JavaGateway(GatewayClient(port=57440), auto_convert=False)
java_import
return gateway
