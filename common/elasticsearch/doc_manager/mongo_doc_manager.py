import re
import copy
import logging
import threading
import asyncio
import elasticsearch_async
from . import constant
from .doc_manager_base import DocManagerBase
from common.elasticsearch import async_helpers
from .formatters import DefaultDocumentFormatter


class DocManager(DocManagerBase):  # todo: 反向处理base
    def __init__(self,
                 hosts,
                 auto_commit_interval=constant.DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id',
                 chunk_size=constant.DEFAULT_MAX_BULK,
                 meta_index_name='mongodb_meta',
                 meta_type='mongodb_meta',
                 attachment_field='content',
                 **kwargs):
        client_options = kwargs.get('client_options')
        if type(hosts) is not list:
            hosts = [hosts]
        self._es = elasticsearch_async.AsyncElasticsearch(hosts=hosts, **client_options)
        self._formatter = DefaultDocumentFormatter()  # todo 验证formatter
        self.bulk_buffer = BulkBuffer(self)
        self.look = threading.Lock()  # todo: 确认实际应用场景

        self.auto_commit_interval = auto_commit_interval
        self.auto_send_interval = kwargs.get('auto_send_interval', constant.DEFAULT_SEND_INTERVAL)
        self.meta_index_name = meta_index_name
        self.meta_type = meta_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self.attachment_field = attachment_field
        self.auto_committer = AutoCommitter(self, self.auto_send_interval, self.auto_commit_interval)
        self.auto_committer.start()

    @staticmethod
    def _index_and_mapping(namespace):
        index, doc_type = namespace.lower().split('.', 1)
        return index, doc_type

    def upsert(self, doc, namespace, timestamp, is_update=False):
        """

        :param doc: native object
        :param namespace:
        :param timestamp:
        :param is_update:
        :return:
        """
        index, doc_type = self._index_and_mapping(namespace)
        doc_id = str(doc.pop('_id'))

        _original_op_type = 'update' if is_update else 'index'
        _op_type = _original_op_type

        doc = self._formatter.format_document(doc)

        pre_action = self.bulk_buffer.get_action(doc_id)

        if pre_action:
            # 处理_op_type:
            # 1. (pre_op_type, _original_op_type)存在'index'，则为'index';
            # 2. 否则为'update'.
            _op_type = 'index' if 'index' in (pre_action.get('_op_type'), _op_type) else _op_type
            # 合并doc
            doc = self.apply_update(pre_action.get('_source', {}), doc)

        action = {
            '_op_type': _op_type,
            '_index': index,
            '_type': doc_type,
            '_id': doc_id,
            '_source': doc
        }

        meta_action = {
            '_index': self.meta_index_name,
            '_type': self.meta_type,
            '_source': {'ns': namespace,
                        '_ts': timestamp,
                        'op': _original_op_type,
                        'doc_id': doc_id,
                        'status': constant.ActionStatus.processing
                        }
        }
        self.index(action, meta_action)

    def delete(self, doc_id, namespace, timestamp):
        index, doc_type = self._index_and_mapping(namespace)
        action = {
            '_op_type': 'delete',
            '_index': index,
            '_type': doc_type,
            '_id': doc_id
        }

        meta_action = {
            '_index': self.meta_index_name,
            '_type': self.meta_type,
            '_source': {'ns': namespace,
                        '_ts': timestamp,
                        'op': 'delete',
                        'doc_id': doc_id,
                        'status': constant.ActionStatus.processing
                        }
        }
        self.index(action, meta_action)

    def bulk_upsert(self):
        """
        Insert multiple documents into Elasticsearch directly.
        :return:
        """
        raise NotImplementedError()

    def index(self, action, meta_action):
        self.bulk_buffer.add_upsert(action, meta_action)

        if self.bulk_buffer.count() >= self.chunk_size or self.auto_commit_interval == 0:
            self.commit()

    def commit(self):
        return asyncio.wait_for(self.send_buffered_operations(), None)

    async def send_buffered_operations(self):
        action_buffer = self.bulk_buffer.get_buffer()
        if action_buffer:
            coro = async_helpers.bulk(client=self._es, actions=action_buffer, max_retries=3, initial_backoff=0.1,
                                      max_backoff=1)
            succeed, failed = await asyncio.wait_for(coro, None)
            print('succeed:', len(succeed), 'failed:', len(failed))
            print(succeed, failed)
            # todo: 持久化记录

    def _search(self):
        pass

    def _get_last_doc(self):
        pass


class AutoCommitter:
    def __init__(self, docman, send_interval, commit_interval, sleep_interval=1):
        pass

    def start(self):
        pass


class BulkBuffer:
    def __init__(self, docman):
        self.docman = docman
        self.action_buffer = {}  # Action buffer for bulk indexing
        self.action_log = []  # Action log for ES operation
        self._i = -1
        self._count = 0

    def _get_i(self):
        self._i += 1
        return self._i

    def count(self):
        return self._count

    def add_upsert(self, action, meta_action):
        """
        兼容
        :param action:
        :param meta_action:
        :return:
        """
        self.bulk_index(action, meta_action)

    def get_action(self, _id):
        return self.action_buffer.get(_id)

    def bulk_index(self, action, meta_action):
        action['_i'] = self._get_i()

        self.action_buffer[str(action.get('_id'))] = action
        self.action_log.append(meta_action)
        self._count += 1

    def reset_action(self, _id):
        self.action_buffer[_id] = {}

    def clean_up(self):
        self.action_buffer = {}

    def get_buffer(self):
        es_buffer = sorted(self.action_buffer.values(), key=lambda ac: ac['_id'])
        self.clean_up()
        return es_buffer
