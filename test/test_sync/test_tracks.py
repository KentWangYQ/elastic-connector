import unittest
import asyncio
import datetime

from common.elasticsearch import doc_manager
from module.sync import tracks


class TracksTest(unittest.TestCase):
    def test_impression_track_index_all(self):
        now = datetime.datetime.now()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tracks.impression_track.index_all())
        print('time used:', datetime.datetime.now() - now)
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.close()
