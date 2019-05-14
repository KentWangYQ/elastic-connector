import unittest
import asyncio
import datetime

from common.elasticsearch.doc_manager import mongo_docman
from module.sync import tracks

import sys
import logging

logger = logging.getLogger('rts')
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)

# create console handler and set level to debug
che = logging.StreamHandler()
che.setLevel(logging.WARNING)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
che.setFormatter(formatter)

# add ch to logger
logger.addHandler(che)
logger.addHandler(ch)


class TracksTest(unittest.TestCase):
    def test_merchant_track_index_all(self):
        now = datetime.datetime.now()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tracks.merchant_sync_manager.index_all())
        loop.run_until_complete(mongo_docman.stop())
        print('time used:', datetime.datetime.now() - now)
        # loop.run_until_complete(asyncio.sleep(5))
        # tasks = asyncio.all_tasks(loop)
        # if tasks:
        #     loop.run_until_complete(asyncio.wait(asyncio.all_tasks(loop)))
        # loop.close()
