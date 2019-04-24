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
            {'_op_type': 'index', '_index': 'rts_test', '_type': 'rt', '_id': 1, 'now': datetime.datetime.now()},
        ]
        future = async_helpers.bulk(client=client, actions=actions, max_retries=3, initial_backoff=0.1, max_backoff=1)
        loop = asyncio.get_event_loop()
        succeed, failed = loop.run_until_complete(future)
        print('succeed:', len(succeed), '\nfailed:', len(failed))
        print(succeed, failed)
        loop.run_until_complete(asyncio.wait(asyncio.Task.all_tasks()))
        loop.close()
