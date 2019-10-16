# Inspired by https://python-guide-pt-br.readthedocs.io/fr/latest/writing/logging.html#logging-in-a-library
# Set default logging handler to avoid "No handler found" warnings.

from wrapper import PandAgg
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
