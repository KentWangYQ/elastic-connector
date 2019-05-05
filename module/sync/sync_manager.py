from inspect import isfunction
from aiostream import stream


class SyncManager:
    def __init__(self, oplog_client, mongo_docman, collection, namespace, query_options=None, *args, **kwargs):
        """

        :param oplog_client:
        :param mongo_docman:
        :param collection:
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
        self.query_options = query_options or {'batch_size': 1000}

    async def index_all(self, doc_process_func=None):
        """
        Whole quantity index from source
        :return:
        """
        cursor = self.collection.find(**self.query_options)
        while True:
            batch = await stream.list(stream.take(cursor, self.query_options.get('batch_size')))
            if not batch:
                break
            print('batch size', len(batch))  # todo: logging化
            if doc_process_func:
                # doc process
                assert isfunction(doc_process_func), 'doc_process_func must be a function!'
                batch = map(doc_process_func, batch)
            self.mongo_docman.bulk_index(batch, self.namespace)

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
            @self.oplog_client.on('%s_insert' % self.collection)
            def on_insert(oplog):
                return self.insert_doc(oplog, doc_process_funcs.get('i'))

        if 'u' in ops:
            @self.oplog_client.on('%s_update' % self.collection)
            def on_update(oplog):
                return self.update_doc(oplog, doc_process_funcs.get('u'))

        if 'd' in ops:
            @self.oplog_client.on('%s_delete' % self.collection)
            def on_delete(oplog):
                return self.delete_doc(oplog, doc_process_funcs.get('d'))
