#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Inspired by https://python-guide-pt-br.readthedocs.io/fr/latest/writing/logging.html#logging-in-a-library
# Set default logging handler to avoid "No handler found" warnings.

import logging

from logging import NullHandler

logging.getLogger(__name__).addHandler(NullHandler())
