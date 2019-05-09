from inspect import isfunction
from module import constant


class SyncManager:
    def __init__(self, oplog_client, mongo_docman, collection, namespace, query_options=None, *args,
                 **kwargs):
        """

        :param oplog_client:
        :param mongo_docman:
        :param collection:
        :param collection_name:
        :param namespace:
        :param query_options:
                - filter
                - skip
                - limit
                - sort
                - batch_size
                - projection
                - pop_fields
                - pipeline
                - all other mongodb collection find params
        :param args:
        :param kwargs:
        """
        self.oplog_client = oplog_client
        self.mongo_docman = mongo_docman
        self.collection = collection
        self.namespace = namespace
        self.query_options = query_options or {}
        if 'batch_size' not in self.query_options:
            self.query_options['batch_size'] = constant.MONGO_BATCH_SIZE
        self.routing = kwargs.get('_routing')

    async def index_all(self, doc_process_func=None):
        """
        Whole quantity index from source
        :return:
        """
        cursor = self.collection.find(sort=[('_id', -1)], **self.query_options)
        await self.mongo_docman.bulk_index(cursor, self.namespace, params={'routing': self.routing})

    def delete_all(self):
        return self.mongo_docman.delete_by_query_sync(namespace=self.namespace, body={"query": {"match_all": {}}})

    def delete_index(self):
        return self.mongo_docman.delete_index(namespace=self.namespace, params={'ignore_unavailable': True})

    def insert_doc(self, oplog, doc_process_func=None):
        """
        Process insert document option
        :param oplog:
        :param doc_process_func: doc pre-process
        :return:
        """
        doc = oplog.get('o')
        if doc_process_func:
            # doc process
            assert isfunction(doc_process_func), 'doc_process_func must be a function!'
            doc = doc_process_func(doc)
        return self.mongo_docman.index(doc, namespace=self.namespace, timestamp=oplog.get('ts'))

    def update_doc(self, oplog, doc_process_func=None):
        """
        Process update document option
        :param oplog:
        :param doc_process_func: doc pre-process
        :return:
        """
        # todo: support doc_process_func
        return self.mongo_docman.update(oplog.get('o2').get('_id'), oplog.get('o'), namespace=self.namespace,
                                        timestamp=oplog.get('ts'))

    def delete_doc(self, oplog, doc_process_func=None):
        """
        Process delete document option
        :param oplog:
        :param doc_process_func: doc pre-process
        :return:
        """
        # todo: support doc_process_func
        return self.mongo_docman.delete(oplog.get('o').get('_id'), namespace=self.namespace, timestamp=oplog.get('ts'))

    def real_time_sync(self, ops=('i', 'u', 'd'), doc_process_funcs=None):
        # todo: 增加log
        if 'i' in ops:
            @self.oplog_client.on('%s_insert' % self.collection.name)
            def on_insert(oplog):
                return self.insert_doc(oplog, doc_process_funcs.get('i'))

        if 'u' in ops:
            @self.oplog_client.on('%s_update' % self.collection.name)
            def on_update(oplog):
                return self.update_doc(oplog, doc_process_funcs.get('u'))

        if 'd' in ops:
            @self.oplog_client.on('%s_delete' % self.collection.name)
            def on_delete(oplog):
                return self.delete_doc(oplog, doc_process_funcs.get('d'))
