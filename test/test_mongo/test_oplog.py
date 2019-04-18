import unittest
import asyncio
from common.elasticsearch.bson_serializer import BSONSerializer
from common.mongo.oplog import Oplog


class OplogTest(unittest.TestCase):
    def test_tail(self):
        oplog = Oplog(limit=10)

        @oplog.on('data')
        def on_data(doc):
            print('event data:', BSONSerializer().dumps(doc))

        @oplog.on('insert')
        def on_insert(doc):
            print('event insert:', doc)

        @oplog.on('actfeesaves_insert')
        def on_coll_insert(doc):
            print('event coll insert:', doc)

        @oplog.on('data')
        def close(_):
            oplog.close()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(oplog.tail())
        loop.close()
