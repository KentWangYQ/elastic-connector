import unittest
import datetime
import asyncio
import bson

import config
from common.elasticsearch import doc_manager
from common.elasticsearch.bson_serializer import BSONSerializer

from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=config.CONFIG.ELASTICSEARCH.get('hosts'),
                   timeout=120,
                   retry_on_timeout=True,
                   sniff_on_start=False,
                   sniff_on_connection_fail=True,
                   sniffer_timeout=60,
                   max_retries=3,
                   serializer=BSONSerializer()
                   )


class MongoDocManagerTest(unittest.TestCase):
    index, doc_type = 'rts_test', 'rt'

    def test_index(self):
        future = doc_manager.mongo_doc_manager.index(index=self.index,
                                                     doc_type=self.doc_type,
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
        # loop.set_debug(True)
        loop.run_until_complete(future)
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.close()

    def test_update(self):
        future = doc_manager.mongo_doc_manager.update(index=self.index,
                                                      doc_type=self.doc_type,
                                                      doc={"ip_address": "221.232.96.97",
                                                           "note": {"t": "This is update test"}},
                                                      id=bson.ObjectId("5cb82deea359465f0ab8c722"))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.close()

    def test_delete(self):
        future = doc_manager.mongo_doc_manager.delete(index=self.index,
                                                      doc_type=self.doc_type,
                                                      id=bson.ObjectId("5cb82deea359465f0ab8c722"))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.close()

    def test_bulk(self):
        es.index(index=self.index, doc_type=self.doc_type, body={'now': datetime.datetime.now()}, id=1)
        es.index(index=self.index, doc_type=self.doc_type, body={'now': datetime.datetime.now()}, id=2)
        future = doc_manager.mongo_doc_manager.bulk([
            {
                'action': 'index',
                'metadata': {
                    '_index': self.index,
                    '_type': self.doc_type,
                    '_id': 3,
                },
                'now': datetime.datetime.now()
            },
            {
                'action': 'update',
                'metadata': {
                    '_index': self.index,
                    '_type': self.doc_type,
                    '_id': '1',
                },
                'now1': datetime.datetime.now()
            },
            {
                'action': 'delete',
                'metadata': {
                    '_index': self.index,
                    '_type': self.doc_type,
                    '_id': 2
                },
            }
        ])  # type:asyncio.Future

        future.add_done_callback(MongoDocManagerTest._cb)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.close()

    @staticmethod
    def _cb(fut):
        print(fut)

    def test_update_by_query(self):
        es.index(index=self.index, doc_type=self.doc_type, body={'now': datetime.datetime.now()}, id=1, refresh=True,
                 wait_for_active_shards='all')
        future = doc_manager.mongo_doc_manager.update_by_query(index=self.index,
                                                               doc={'now1': datetime.datetime.now()},
                                                               query={'term': {'_id': '1'}}, doc_type=self.doc_type)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.close()

    def test_delete_by_query(self):
        es.index(index=self.index, doc_type=self.doc_type, body={'now': datetime.datetime.now()}, id=1, refresh=True,
                 wait_for_active_shards='all')
        future = doc_manager.mongo_doc_manager.delete_by_query(index=self.index,
                                                               query={'term': {'_id': '1'}}, doc_type=self.doc_type)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.run_until_complete(doc_manager.mongo_doc_manager._es.transport.close())
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        loop.close()
