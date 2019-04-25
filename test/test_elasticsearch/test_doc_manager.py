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
    TESTARGS = ("rts_test.rt", 1)

    def _run(self, future):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        # loop.run_until_complete(doc_manager.mongo_dm._es.transport.close())
        # loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        # loop.close()

    def test_upsert(self):
        doc_manager.mongo_dm.index({'_id': 1, 'now': datetime.datetime.now(), 'a': 1, 'b': 2}, *self.TESTARGS)
        doc_manager.mongo_dm.update({'_id': 1, 'now': datetime.datetime.now(), 'a': 3, 'b': 4}, *self.TESTARGS)
        doc_manager.mongo_dm.update({'_id': 1, '$set': {'a': 5, 'b': 6}}, *self.TESTARGS)

        doc_manager.mongo_dm.update({'_id': 1, '$unset': {'a': 1}}, *self.TESTARGS)
        future = doc_manager.mongo_dm.commit()
        self._run(future)

    def test_delete(self):
        _id = 1
        doc_manager.mongo_dm.index({'_id': _id, 'now': datetime.datetime.now(), 'a': 1, 'b': 2}, *self.TESTARGS)
        doc_manager.mongo_dm.delete(_id, *self.TESTARGS)
        future = doc_manager.mongo_dm.commit()
        self._run(future)

    def test_auto_committer(self):
        mongo_doc_manager = doc_manager.mongo_doc_manager.DocManager(hosts=config.CONFIG.ELASTICSEARCH.get('hosts'),
                                                                     client_options={
                                                                         'timeout': 120,
                                                                         'retry_on_timeout': True,
                                                                         'sniff_on_start': False,
                                                                         'sniff_on_connection_fail': True,
                                                                         'sniffer_timeout': 60,
                                                                         'max_retries': 3,
                                                                         'serializer': BSONSerializer()
                                                                     },
                                                                     auto_commit_interval=-1,
                                                                     auto_commit=True,  # start auto committer
                                                                     )
        mongo_doc_manager.index({'_id': 1, 'now': datetime.datetime.now(), 'a': 1, 'b': 2}, *self.TESTARGS)
        # stop auto commit
        asyncio.ensure_future(mongo_doc_manager.stop())

        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))
        # loop.close()
