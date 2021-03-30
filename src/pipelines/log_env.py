import logging
import os

LOGLEVEL = os.environ.get('LOGLEVEL', 'WARN').upper()
logging.basicConfig(level=LOGLEVEL)
