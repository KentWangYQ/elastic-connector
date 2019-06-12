import pydash as _
import logging
from functools import partial

logger = logging.getLogger('rts')


def parent_doc_pro(doc, parent_key):
    # if _.has(doc, parent_key):
    doc['_parent'] = _.get(doc, parent_key)
    return doc


def create_index(mongd_docman, index, mappings, settings):
    if not mongd_docman.es_sync.indices.exists(index):
        res = mongd_docman.es_sync.indices.create(index=index, body={
            'mappings': mappings,
            'settings': settings
        })
        if not res or not res.get('acknowledged') is True:
            logger.error('Create indies failed. %r', res)
            raise Exception('Create indies failed', res)


async def index_all(managers):
    for manager in managers:
        await manager.index_all()


async def delete_all(managers):
    for manager in managers:
        await manager.delete_all()


def delete_index(managers):
    for manager in managers:
        manager.delete_index()


def real_time_sync(managers):
    for manager in managers:
        manager.real_time_sync()


def gen_func(mongd_docman, index, mappings, settings, managers):
    return partial(create_index, mongd_docman, index, mappings, settings), \
           partial(index_all, managers), \
           partial(delete_all, managers), \
           partial(delete_index, managers), \
           partial(real_time_sync, managers)
