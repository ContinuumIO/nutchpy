from __future__ import absolute_import, division, print_function

"""Nutch python module"""

import logging
from .JVM import gateway

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

gateway = gateway

from .readers import *

