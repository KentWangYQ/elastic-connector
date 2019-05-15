import unittest
import asyncio
from common.elasticsearch.bson_serializer import BSONSerializer
from common.mongo.oplog import Oplog


class OplogTest(unittest.TestCase):
    def test_tail(self):
        oplog = Oplog(limit=10)

        @oplog.on('data')
        async def on_data(doc):
            await asyncio.sleep(.1)
            print('event data:', BSONSerializer().dumps(doc))

        @oplog.on('insert')
        def on_insert(doc):
            print('event insert:', doc)

        @oplog.on('actfeesaves_insert')
        def on_coll_insert(doc):
            print('event coll insert:', doc)

        async def close():
            await asyncio.sleep(5)
            oplog.close()

        asyncio.ensure_future(close())

        loop = asyncio.get_event_loop()
        loop.run_until_complete(oplog.tail())
        loop.close()

    def test_earliest_ts(self):
        oplog = Oplog()
        print(oplog.earliest_ts())
