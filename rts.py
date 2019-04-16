# -*- coding: utf-8 -*-

import asyncio
import pymongo

import motor.motor_asyncio
import elasticsearch_async

from common.bson_serializer import BSONSerializer
from config import CONFIG


async def tail(coll_oplog, es_client):
    while True:
        cursor = coll_oplog.find(cursor_type=pymongo.CursorType.TAILABLE,
                                 no_cursor_timeout=True,
                                 batch_size=100)
        while True:
            if not cursor.alive:
                await asyncio.sleep(1)
                break

            if await cursor.fetch_next:
                op = cursor.next_object()
                if op and 'o' in op:
                    print('OPLOG:', op)
                    await index(es_client, op.get('o'))


async def index(client: elasticsearch_async.AsyncElasticsearch, body: dict):
    await asyncio.sleep(1)
    rst = await client.index(index='rts_test', doc_type='rt', body=body)
    print('index result:', rst)


async def es_client_close(client: elasticsearch_async.AsyncElasticsearch):
    await client.transport.close()


async def main():
    oplog_client = motor.motor_asyncio.AsyncIOMotorClient(CONFIG.MONGO.get('oplog_uri'))
    coll_oplog = oplog_client.local.oplog.rs
    es_client = elasticsearch_async.AsyncElasticsearch(hosts=CONFIG.ELASTICSEARCH.get('hosts'),
                                                       timeout=120,
                                                       retry_on_timeout=True,
                                                       sniff_on_start=True,
                                                       sniff_on_connection_fail=True,
                                                       sniffer_timeout=1200,
                                                       max_retries=3,
                                                       serializer=BSONSerializer()
                                                       )

    await tail(coll_oplog, es_client)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
