import logging
import bson
import asyncio
import pymongo
import motor.motor_asyncio

import config
from common.event_emitter import EventEmitter

logger = logging.getLogger('rts')


def _filter(*, ts=None, ns=None, include_ns=None, exclude_ns=None, op=None, include_ops=None, exclude_ops=None):
    q = {}
    if isinstance(ts, bson.Timestamp):
        q['ts'] = {'$gt': ts}

    # ns
    q_ns = []
    if isinstance(ns, str):
        if ns.endswith('.*'):
            q_ns.append({'ns': {'$regex': '^{0}\\.'.format(ns.replace('.*', ''))}})
        else:
            q_ns.append({'ns': ns})
    if isinstance(include_ns, list):
        q_ns.append({'ns': {'$in': include_ns}})
    if isinstance(exclude_ns, list):
        q_ns.append({'ns': {'$nin': exclude_ns}})
    if len(q_ns) > 0:
        q['$and'] = q_ns

    # op
    if isinstance(op, str):
        q['op'] = op
    elif isinstance(include_ops, list):
        q['op'] = {'$in': include_ops}
    elif isinstance(exclude_ops, list):
        q['op'] = {'$nin': exclude_ops}

    return q


class Oplog(EventEmitter):
    def __init__(self, uri=None, batch_size=100, skip=0, limit=0, **kwargs):
        super().__init__()
        self._close = False
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri or config.CONFIG.MONGO.get('oplog_uri'))
        self._coll_oplog = self._client.local.oplog.rs
        self._filter = _filter(**kwargs)
        self._batch_size = batch_size
        self._skip = skip
        self._limit = limit

        @self.on('i_error')
        def i_error(e, *args, **kwargs):
            logger.warning('Oplog listener process error', e, *args, **kwargs)
            self.emit('error', e, *args, **kwargs)

    _op_mapping = {
        'i': 'insert',
        'u': 'update',
        'd': 'delete',
        'c': 'cmd',
        'n': 'noop'
    }

    async def tail(self):
        while not self._close:
            cursor = self._coll_oplog.find(filter=self._filter,
                                           cursor_type=pymongo.CursorType.TAILABLE,
                                           no_cursor_timeout=True,
                                           batch_size=self._batch_size,
                                           skip=self._skip,
                                           limit=self._limit)
            while not self._close and cursor.alive:
                async for doc in cursor:
                    if self._close:
                        return
                    try:
                        # emit event 'data'
                        self.emit('data', doc)
                        # emit event 'op'
                        self.emit(self._op_mapping[doc.get('op')], doc)
                        # emit event 'coll_op'
                        self.emit('%s_%s' % (Oplog._get_coll(doc.get('ns')), self._op_mapping[doc.get('op')]), doc)
                        self._filter['ts'] = doc['ts']
                    except Exception as e:
                        logger.warning('Oplog tail error', e)
                        self.emit('error', e, doc)

            await asyncio.sleep(1)
            cursor.close()
        logger.info('Oplog tail closed')

    def close(self):
        logger.info('Oplog tail closing ...')
        self._close = True

    @staticmethod
    def _get_coll(ns):
        ns_l = ns.split('.')
        ns_l.pop(0)
        if len(ns_l) == 1:
            return ns_l[0]
        else:
            return '.'.join(ns_l)
