import unittest
import datetime
import asyncio
import bson
from common.elasticsearch import doc_manager


class MongoDocManagerTest(unittest.TestCase):
    def test_index(self):
        future = doc_manager.mongo_doc_manager.index(index='rts_test',
                                                     doc_type='rt',
                                                     doc={
                                                         "_id": bson.ObjectId("5cb82deea359465f0ab8c722"),
                                                         "create_time": datetime.datetime(2019, 3, 6, 3, 12, 45,
                                                                                          371000),
                                                         "update_time": datetime.datetime(2019, 3, 6, 3, 12, 45,
                                                                                          371000),
                                                         "ip_address": "221.232.96.96"
                                                     },
                                                     id=bson.ObjectId("5cb82deea359465f0ab8c722"))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        asyncio.wait(asyncio.Task.all_tasks())
