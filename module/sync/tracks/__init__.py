import logging
import pydash as _
from common.mongo import oplog_client
from common.elasticsearch.doc_manager import mongo_docman
from model import merchant, impression_track, act_share_detail
from ..sync_manager import SyncManager
from module.constant import mapping

from . import track_util

logger = logging.getLogger('rts')

_map = mapping.tracks
_index = _map.get('index')
_types = _map.get('types')
_routing = _map.get('routing')
_mappings = _map.get('mappings')
_settings = _map.get('settings')

merchant_sync_manager = SyncManager(oplog_client,
                                    mongo_docman,
                                    collection=merchant,
                                    index=_index,
                                    type=_types.get('merchant').get('type'),
                                    query_options={
                                        'projection': {'createTime': 0, 'creater': 0, 'updater': 0},
                                        # 'batch_size': 500
                                    },
                                    routing=_routing)

impression_track_sync_manager = SyncManager(oplog_client,
                                            mongo_docman,
                                            collection=impression_track,
                                            index=_index,
                                            type=_types.get('impression_track').get('type'),
                                            routing=_routing)

act_share_detail_sync_manager = SyncManager(oplog_client,
                                            mongo_docman,
                                            collection=act_share_detail,
                                            index=_index,
                                            type=_types.get('act_share_detail').get('type'),
                                            routing=_routing)


def _it_doc_process(doc):
    doc = {**doc, **track_util.url_split(doc.get('uri'))}
    if doc.get('senderToUserId') and _.get(doc, 'params.sender'):
        doc['salesId'] = str(_.get(doc, 'params.sender'))
        doc['params']['sender'] = str(doc['senderToUserId'])
    if _.has(doc, 'params.secondlevel'):
        doc['_parent'] = _.get(doc, 'params.secondlevel')
    return doc


def create_index():
    if not mongo_docman.es_sync.indices.exists(_index):
        res = mongo_docman.es_sync.indices.create(index=_index, body={
            'mappings': _mappings,
            'settings': _settings
        })
        if not res or not res.get('acknowledged') is True:
            logger.error('Create indies failed. %r', res)
            raise Exception('Create indies failed', res)


async def index_all():
    await merchant_sync_manager.index_all()
    await impression_track_sync_manager.index_all(doc_process_func=_it_doc_process)
    await act_share_detail_sync_manager.index_all(doc_process_func=_it_doc_process)


async def delete_all():
    await merchant_sync_manager.delete_all()
    await impression_track_sync_manager.delete_all()
    await act_share_detail_sync_manager.delete_all()


def delete_indies():
    merchant_sync_manager.delete_index()
    impression_track_sync_manager.delete_index()
    act_share_detail_sync_manager.delete_index()


def real_time_sync():
    merchant_sync_manager.real_time_sync()
    impression_track_sync_manager.real_time_sync(doc_process_funcs={'i': _it_doc_process})
    act_share_detail_sync_manager.real_time_sync(doc_process_funcs={'i': _it_doc_process})
