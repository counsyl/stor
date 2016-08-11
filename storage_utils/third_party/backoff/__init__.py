__version__ = "1.1.0"
import logging
from logging import NullHandler

logger = logging.getLogger(__name__).addHandler(NullHandler())
