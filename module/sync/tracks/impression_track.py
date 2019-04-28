import model
from common.mongo import oplog_client
from common.elasticsearch.doc_manager import mongo_docman

index, doc_type = 'rts_test', 'rt'
namespace = 'rts_test.rt'


async def index_all():
    cursor = model.impression_track.find(batch_size=1000, limit=100000)
    async for doc in cursor:
        mongo_docman.index(doc, namespace)
    # while True:
    #     batch = await stream.list(stream.take(cursor, 1000))
    #     if not batch:
    #         break
    #     print('batch size', len(batch))
    #     # mongo_doc_manager.bulk(batch, index=index, doc_type=doc_type, action='index')
    #     for doc in batch:
    #         mongo_docman.index(doc, namespace)


def rt():
    @oplog_client.on('impressiontracks_insert')
    def on_insert(data):
        doc = data.get('o')
        mongo_docman.index(doc, namespace=namespace, timestamp=data.get('ts'))
