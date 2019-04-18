# -*- coding: utf-8 -*-

import asyncio
import pymongo
import datetime

import motor.motor_asyncio
import elasticsearch_async
import elasticsearch

from common.elasticsearch.bson_serializer import BSONSerializer
from config import CONFIG


async def tail(coll_oplog, es_client):
    while True:
        cursor = coll_oplog.find(filter={'op': 'i'},
                                 cursor_type=pymongo.CursorType.TAILABLE,
                                 no_cursor_timeout=True,
                                 batch_size=100,
                                 skip=5000,
                                 limit=10000)
        while True:
            if not cursor.alive:
                await asyncio.sleep(1)
                break
            async for op in cursor:
                if op and 'o' in op:
                    print('OPLOG:', op)
                    es_client.index(index='rts_test', doc_type='rt', body=op)
                    # await index(es_client, op.get('o'))
                    # await asyncio.ensure_future(index(es_client, op.get('o')))
                    # asyncio.get_event_loop().call_soon(index_sync, es_client, op.get('o'))
                    # return op

            # if await cursor.fetch_next:
            #     op = cursor.next_object()
            #     if op and 'o' in op:
            #         print('OPLOG:', op)
            #         # await index(es_client, op.get('o'))
            #         asyncio.get_event_loop().call_soon(index, es_client, op.get('o'))
        break


def p():
    print('doing')


def index_sync(client: elasticsearch.Elasticsearch, body: dict):
    rst = client.index(index='rts_test', doc_type='rt', body=body)
    print('index result:', rst)


async def index(client: elasticsearch_async.AsyncElasticsearch, body: dict):
    await asyncio.sleep(1)
    rst = await client.index(index='rts_test', doc_type='rt', body=body)
    print('index result:', rst)


async def es_client_close(client: elasticsearch_async.AsyncElasticsearch):
    await client.transport.close()


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

es_client_sync = elasticsearch.Elasticsearch(hosts=CONFIG.ELASTICSEARCH.get('hosts'),
                                             timeout=120,
                                             retry_on_timeout=True,
                                             sniff_on_start=True,
                                             sniff_on_connection_fail=True,
                                             sniffer_timeout=1200,
                                             max_retries=3,
                                             serializer=BSONSerializer()
                                             )


async def main():
    await tail(coll_oplog, es_client_sync)
    # result = await asyncio.wait(asyncio.Task.all_tasks())
    # for r in result:
    #     print(r.result())
    # await es_client_close(es_client)
    # es_client_sync.transport.close()


now = datetime.datetime.now()
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.run_until_complete(es_client.transport.close())
loop.run_until_complete(asyncio.wait(asyncio.Task.all_tasks(), return_when=asyncio.ALL_COMPLETED))

# loop.close()
# es_client.transport.close()
# loop.run_until_complete(asyncio.wait(asyncio.Task.all_tasks(), return_when=asyncio.ALL_COMPLETED))
# es_client_sync.transport.close()
# asyncio.ensure_future(main())
# loop.run_forever()
print('Time used:', datetime.datetime.now() - now)
