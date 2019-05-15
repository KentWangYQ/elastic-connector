import os
import sys
import logging
from . import base, test

RTS_ENV = str.lower(os.getenv('RTS_ENV') or 'test')
RTS_DEBUG = str.lower(os.getenv('RTS_DEBUG') or '') == 'true'

__config = {
    'base': base.Config,
    'test': test.Config
}

CONFIG = __config[RTS_ENV]

logger = logging.getLogger('rts')
logger.setLevel(logging.DEBUG if RTS_DEBUG else logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

che = logging.StreamHandler()
che.setLevel(logging.WARNING)
che.setFormatter(formatter)

logger.addHandler(che)
logger.addHandler(ch)
