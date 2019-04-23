import unittest
import asyncio
import datetime
import config

from common.elasticsearch.bson_serializer import BSONSerializer
from common.elasticsearch import async_helpers
from elasticsearch_async import AsyncElasticsearch


class AsyncHelpersTest(unittest.TestCase):
    @staticmethod
    def streaming_bulk_done_callback(future, succeed, faild, *args, **kwargs):
        pass

    def test_streaming_bulk(self):
        client = AsyncElasticsearch(hosts=config.CONFIG.ELASTICSEARCH.get('hosts'),
                                    timeout=120,
                                    retry_on_timeout=True,
                                    sniff_on_start=False,
                                    sniff_on_connection_fail=True,
                                    sniffer_timeout=60,
                                    max_retries=3,
                                    serializer=BSONSerializer())

        actions = [
            {'_op_type': 'index', '_index': 'rts_test', '_type': 'rt', '_id': 1, 'now': datetime.datetime.now()},
            {'_op_type': 'index', '_index': 'rts_test', '_type': 'rt', '_id': 2, 'now': datetime.datetime.now()},
        ]
        future = async_helpers.streaming_bulk(client=client, actions=actions,
                                              done_callback=AsyncHelpersTest.streaming_bulk_done_callback)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(*future)
        loop.run_until_complete(asyncio.wait(asyncio.Task.all_tasks()))
        loop.close()
