import unittest
import asyncio
import datetime

from common.elasticsearch.doc_manager import mongo_docman
from module.sync import tracks


class TracksTest(unittest.TestCase):
    def test_impression_track_index_all(self):
        now = datetime.datetime.now()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tracks.impression_track.index_all())
        loop.run_until_complete(mongo_docman.stop())
        print('time used:', datetime.datetime.now() - now)
        # loop.run_until_complete(asyncio.sleep(5))
        # loop.run_until_complete(asyncio.wait(asyncio.all_tasks(loop)))
        loop.close()
