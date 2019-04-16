import os
from . import base, test

RTS_ENV = str.lower(os.getenv('RTS_ENV') or 'test')

__config = {
    'base': base.Config,
    'test': test.Config
}

CONFIG = __config[RTS_ENV]
