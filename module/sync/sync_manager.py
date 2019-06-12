import logging
from aiostream import stream
from functools import partial
from inspect import isfunction
from module import constant

logger = logging.getLogger('rts')


class QueryOption:
    def __init__(self, pipeline=None, pipeline_params=None,
                 pop_fields=None, pop_params=None,
                 query_filter=None, query_params=None,
                 batch_size=None, doc_pre_process=None):
        """

        :param pipeline:
        :param pipeline_params:
        :param pop_fields:
        :param pop_params:
        :param query_filter:
        :param query_params:
        :param batch_size:
        :param doc_pre_process: { 'i':{ func: lambda x: x, params: {'field1':1, field2: 2} },
                                  'u':{ func: lambda x: x, params: {'field1':1, field2: 2} }
                                  }
        """
        self.pipeline = pipeline
        self.pipeline_params = pipeline_params
        self.pop_fields = pop_fields
        self.pop_params = pop_params
        self.query_filter = query_filter
        self.query_params = query_params
        self.batch_size = batch_size or constant.MONGO_BATCH_SIZE
        self.doc_pre_process = doc_pre_process or {}
        self.mode = 'aggregate' if pipeline else 'populates' if pop_fields else 'find'
        self.cmd = pipeline if pipeline else pop_fields if pop_fields else query_filter or {}
        self.params = (pipeline_params if pipeline else pop_params if pop_fields else query_params) or {}


class SyncManager:
    def __init__(self, oplog_client, mongo_docman, collection, index, doc_type, routing=None, query_options=None):
        """

        :param oplog_client:
        :param mongo_docman:
        :param collection:
        :param index:
        :param doc_type:
        :param routing:
        :param query_options:
        """
        self.oplog_client = oplog_client
        self.mongo_docman = mongo_docman
        self.collection = collection
        self.namespace = '.'.join([index, doc_type])
        self.routing = routing
        self.query_options = query_options or QueryOption()

    async def index_all(self):
        """
        Whole quantity index from source
        :return:
        """
        cursor = getattr(self.collection, self.query_options.mode)(self.query_options.cmd, **self.query_options.params)
        doc_process_func = None
        dpp = self.query_options.doc_pre_process.get('i')
        if dpp:
            doc_process_func = partial(dpp.get('func'), **(dpp.get('params') or {}))
        await self.mongo_docman.bulk_index(cursor, self.namespace, params={'routing': self.routing},
                                           doc_process=doc_process_func)

    async def delete_all(self):
        await self.mongo_docman.delete_by_query(namespace=self.namespace, body={"query": {"match_all": {}}})

    def delete_index(self):
        return self.mongo_docman.delete_index(namespace=self.namespace, params={'ignore_unavailable': True})

    async def _query_by_id(self, _id):
        doc = None
        if self.query_options.mode in ['aggregate', 'populates']:
            if self.query_options.mode == 'aggregate':
                self.query_options.cmd.insert(0, {'$match': {'_id': _id}})
            elif self.query_options.mode == 'populates':
                self.query_options.params['filter_'] = self.query_options.params.get('filter_', {})
                self.query_options.params['filter_'].update({'_id': _id})

            try:
                doc = await stream.getitem(
                    getattr(self.collection, self.query_options.mode)(self.query_options.cmd,
                                                                      **self.query_options.params),
                    0)
            except IndexError:
                doc = None
        return doc

    async def insert_doc(self, oplog, doc_process_func=None):
        """
        Process insert document option
        :param oplog:
        :param doc_process_func: doc pre-process
        :return:
        """
        _c = self.mongo_docman.bulk_buffer.global_counter()
        doc = oplog.get('o')
        if self.query_options.mode in ['aggregate', 'populates']:
            _id = doc.get('_id')
            doc = await self._query_by_id(_id)

        if doc:
            doc['_i'] = _c
            if doc_process_func:
                # doc process
                assert isfunction(doc_process_func), 'doc_process_func must be a function!'
                doc = doc_process_func(doc)
            if self.routing:
                doc['_routing'] = self.routing
            return self.mongo_docman.index(doc, namespace=self.namespace, timestamp=oplog.get('ts'))
        else:
            return None

    async def update_doc(self, oplog, doc_process_func=None):
        """
        Process update document option
        :param oplog:
        :param doc_process_func: doc pre-process
        :return:
        """
        _id = oplog.get('o2').get('_id')
        doc = oplog.get('o')
        if doc_process_func:
            assert isfunction(doc_process_func), 'doc_process_func must be a function!'
            _id, doc = doc_process_func(_id, doc)

        if self.query_options.mode in ['aggregate', 'populates']:
            doc = await self._query_by_id(_id)
        if doc:
            if self.routing:
                doc['_routing'] = self.routing
            return self.mongo_docman.update(_id, doc, namespace=self.namespace, timestamp=oplog.get('ts'))
        else:
            return None

    def delete_doc(self, oplog):
        """
        Process delete document option
        :param oplog:
        :return:
        """
        return self.mongo_docman.delete(doc={'_id': oplog.get('o').get('_id'), '_routing': self.routing},
                                        namespace=self.namespace, timestamp=oplog.get('ts'))

    def real_time_sync(self, ops=('i', 'u', 'd')):
        logger.info(f'[RTS register]: ns: {self.namespace} op: {".".join(ops)}')

        if 'i' in ops:
            doc_process_func = None
            dpp = self.query_options.doc_pre_process.get('i')
            if dpp:
                doc_process_func = partial(dpp.get('func'), **(dpp.get('params') or {}))

            @self.oplog_client.on('%s_insert' % self.collection.name)
            async def on_insert(oplog):
                return await self.insert_doc(oplog, doc_process_func)

        if 'u' in ops:
            doc_process_func = None
            dpp = self.query_options.doc_pre_process.get('u')
            if dpp:
                doc_process_func = partial(dpp.get('func'), **(dpp.get('params') or {}))

            @self.oplog_client.on('%s_update' % self.collection.name)
            async def on_update(oplog):
                return await self.update_doc(oplog, doc_process_func)

        if 'd' in ops:
            @self.oplog_client.on('%s_delete' % self.collection.name)
            def on_delete(oplog):
                return self.delete_doc(oplog)
