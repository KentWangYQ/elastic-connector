from aiostream import stream
import model
from common.mongo import oplog_client
from common.elasticsearch.doc_manager import mongo_doc_manager

index, doc_type = 'rts_test', 'rt'


async def index_all():
    cursor = model.impression_track.find(batch_size=10000, limit=100000)
    while True:
        batch = await stream.list(stream.take(cursor, 10000))
        if not batch:
            break
        print('batch size', len(batch))
        mongo_doc_manager.bulk(batch, index=index, doc_type=doc_type, action='index')


def rt():
    @oplog_client.on('impressiontracks_insert')
    def on_insert(data):
        doc = data.get('o')
        _id = doc.pop('_id')
        mongo_doc_manager.index(index=index, doc_type=doc_type, doc=doc, id=_id)
