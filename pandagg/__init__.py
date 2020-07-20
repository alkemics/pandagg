#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from logging import NullHandler

# ensures all required classes are registered in DSLMeta
from .interactive import *
from .search import *

# Inspired by https://python-guide-pt-br.readthedocs.io/fr/latest/writing/logging.html#logging-in-a-library
# Set default logging handler to avoid "No handler found" warnings.
logging.getLogger(__name__).addHandler(NullHandler())

__all__ = []
