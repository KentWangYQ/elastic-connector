import threading
import asyncio
import elasticsearch_async
from common.elasticsearch import async_helpers
from common.elasticsearch.action_log import SVActionLogBlock, SVActionLogBlockStatus, GENESIS_BLOCK
from common.elasticsearch.bson_serializer import BSONSerializer
from . import constant
from .doc_manager_base import DocManagerBase
from .formatters import DefaultDocumentFormatter


class ElasticOperate:
    index = 'index'
    update = 'update'
    delete = 'delete'


class DocManager(DocManagerBase):
    def __init__(self,
                 hosts,
                 auto_commit_interval=constant.DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id',
                 chunk_size=constant.DEFAULT_MAX_BULK,
                 log_index='mongodb_log',
                 log_type='mongodb_log',
                 error_index='mongodb_error',
                 error_type='mongodb_error',
                 attachment_field='content',
                 auto_commit=False,
                 **kwargs):
        client_options = kwargs.get('client_options')
        if type(hosts) is not list:
            hosts = [hosts]
        self._es = elasticsearch_async.AsyncElasticsearch(hosts=hosts, **client_options)
        self._formatter = DefaultDocumentFormatter()  # todo 验证formatter
        self.bulk_buffer = BulkBuffer(self)  # todo 搞定prev_block
        self.look = threading.Lock()  # todo: 确认实际应用场景

        # auto_commit_interval < 0: do not commit automatically
        # auto_commit_interval = 0: commit each request;
        # auto_commit_interval > 0: auto commit every auto_commit_interval seconds
        self.auto_commit_interval = auto_commit_interval
        # auto_send_interval < 0: do not send automatically
        # auto_send_interval = 0: send each request;
        # auto_send_interval > 0: auto send every auto_commit_interval seconds
        self.auto_send_interval = kwargs.get('auto_send_interval', constant.DEFAULT_SEND_INTERVAL)
        self.log_index = log_index
        self.log_type = log_type
        self.error_index = error_index
        self.error_type = error_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self.attachment_field = attachment_field
        self.auto_committer = AutoCommitter(self, self.auto_send_interval, self.auto_commit_interval)
        if auto_commit:
            asyncio.ensure_future(self.auto_committer.run())

    @staticmethod
    def _index_and_mapping(namespace):
        """
        Namespace to index and doc_type
        :param namespace:
        :return:
        """
        index, doc_type = namespace.lower().split('.', 1)
        return index, doc_type

    def _upsert(self, doc, namespace, timestamp, is_update=False):
        """
        index or update document
        :param doc: native object
        :param namespace:
        :param timestamp:
        :param is_update:
        :return:
        """
        index, doc_type = self._index_and_mapping(namespace)
        doc_id = str(doc.pop('_id'))
        doc['doc_id'] = doc_id

        _original_op_type = ElasticOperate.update if is_update else ElasticOperate.index
        _op_type = _original_op_type

        doc = self._formatter.format_document(doc)

        pre_action = self.bulk_buffer.get_action(doc_id)

        if pre_action:
            # 处理_op_type:
            # 1. (pre_op_type, _original_op_type)存在'index'，则为'index';
            # 2. 否则为'update'.
            _op_type = ElasticOperate.index if (
                    ElasticOperate.index in (pre_action.get('_op_type'), _op_type)) else _op_type
            # 合并doc
            doc = self.apply_update(pre_action.get('_source', {}), doc)

        action = {
            '_op_type': _op_type,
            '_index': index,
            '_type': doc_type,
            '_id': doc_id,
            '_source': doc
        }
        action_log = {**{'ns': namespace,
                         '_ts': timestamp,
                         'op': _original_op_type
                         },
                      **doc}
        self._push_to_buffer(action, action_log)

    def _push_to_buffer(self, action, action_log):
        """Push action and action_log to buffer

        If buffer size larger than chunk size, commit buffered actions to Elasticsearch
        :param action:
        :param action_log:
        :return:
        """
        # push action to buffer
        self.bulk_buffer.add_action(action, action_log)

        #
        if self.bulk_buffer.count() >= self.chunk_size or self.auto_commit_interval == 0:
            # commit
            self.commit()

    def index(self, doc, namespace, timestamp):
        """
        Index document
        :param doc:
        :param namespace:
        :param timestamp:
        :return:
        """
        self._upsert(doc, namespace, timestamp)

    def update(self, doc, namespace, timestamp):
        """
        Update document
        :param doc:
        :param namespace:
        :param timestamp:
        :return:
        """
        self._upsert(doc, namespace, timestamp, is_update=True)

    def delete(self, doc_id, namespace, timestamp):
        """
        Delete document by doc_id
        :param doc_id:
        :param namespace:
        :param timestamp:
        :return:
        """
        index, doc_type = self._index_and_mapping(namespace)
        _op_type = 'delete'
        action = {
            '_op_type': _op_type,
            '_index': index,
            '_type': doc_type,
            '_id': doc_id
        }

        action_log = {
            'ns': namespace,
            '_ts': timestamp,
            'op': _op_type,
            'doc_id': doc_id
        }
        self._push_to_buffer(action, action_log)

    def bulk_upsert(self):
        """
        Insert multiple documents into Elasticsearch directly.
        :return:
        """
        raise NotImplementedError()

    async def send_buffered_actions(self):
        """Send buffered actions to Elasticsearch"""
        if self.bulk_buffer.count() > 0:
            # get action buffer and operate log block
            action_buffer, action_log_block = self.bulk_buffer.get_buffer()

            # coroutine for index log block
            # todo: use es helper which support retry
            logs_future = self._send_action_log_block(action_log_block)
            # coroutine for bulk actions
            actions_future = asyncio.ensure_future(
                async_helpers.bulk(client=self._es, actions=action_buffer, max_retries=3,
                                   initial_backoff=0.1,
                                   max_backoff=1))
            # wait for coro complete
            await asyncio.wait([logs_future, actions_future])

            # bulk result which is a tuple for succeed and failed actions: (succeed, failed)
            succeed, failed = actions_future.result()
            print('succeed:', len(succeed), 'failed:', len(failed))
            print(succeed, failed)

            # commit log block result
            await self._commit_log(action_log_block, failed)

            # todo: 记录 failed

    def _send_action_log_block(self, block):
        action_log = {
            '_op_type': ElasticOperate.index,
            '_index': self.log_index,
            '_type': self.log_type,
            '_id': block.id,
            '_source': block.to_dict()
        }
        return asyncio.ensure_future(async_helpers.bulk(client=self._es, actions=[action_log], max_retries=3,
                                                        initial_backoff=0.1,
                                                        max_backoff=1))

    def _commit_log(self, block, failed):
        def _emf(info):
            return {
                '_op_type': ElasticOperate.index,
                '_index': self.error_index,
                '_type': self.error_type,
                '_source': info
            }

        log_commit = {
            '_op_type': ElasticOperate.update,
            '_index': self.log_index,
            '_type': self.log_type,
            '_id': block.id,
            '_source': {'doc': {'status': SVActionLogBlockStatus.done}}
        }
        actions = [log_commit]
        actions.extend(map(_emf, failed))
        return asyncio.ensure_future(async_helpers.bulk(client=self._es, actions=actions, max_retries=3,
                                                        initial_backoff=0.1,
                                                        max_backoff=1))

    async def commit(self):
        """Send bulk buffer to Elasticsearch, then refresh."""
        # send
        await self.send_buffered_actions()
        # commit
        await self._es.indices.refresh()

    async def stop(self):
        """Stop auto committer"""
        await self.auto_committer.stop()

    def handle_command(self, command_doc, namespace, timestamp):
        raise NotImplementedError()

    def search(self):
        pass


class AutoCommitter:
    def __init__(self, docman, send_interval=0, commit_interval=0, sleep_interval=1):
        self.docman = docman
        self._send_interval = send_interval
        self._commit_interval = commit_interval
        self._auto_send = self._send_interval > 0
        self._auto_commit = self._commit_interval > 0
        self._sleep_interval = sleep_interval
        self._stopped = False

    async def run(self):
        while not self._stopped:
            if self._auto_commit:
                await self.docman.commit()

            elif self._auto_send:
                await self.docman.send_buffered_actions()

            await asyncio.sleep(self._sleep_interval)

    async def stop(self):
        self._stopped = True


class BulkBuffer:
    def __init__(self, docman, prev_block=GENESIS_BLOCK):
        self.docman = docman  # doc manager
        self.action_buffer = {}  # Action buffer for bulk indexing
        self.action_logs = []  # Action log for ES operation
        self._i = -1  # priority for action
        self._sv_block_chain = SVBlockChain(head=prev_block)

    def _get_i(self):
        # todo: 多进程需要加锁
        self._i += 1
        return self._i

    def count(self):
        return len(self.action_buffer)

    def add_action(self, action, action_log):
        """
        兼容
        :param action:
        :param action_log:
        :return:
        """
        self.bulk_index(action, action_log)

    def get_action(self, _id):
        return self.action_buffer.get(_id)

    def bulk_index(self, action, action_log):
        action['_i'] = self._get_i()

        self.action_buffer[str(action.get('_id'))] = action
        self.action_logs.append(action_log)

    def reset_action(self, _id):
        self.action_buffer[_id] = {}

    def clean_up(self):
        self.action_buffer = {}
        self.action_logs = []

    def get_buffer(self):
        if self.action_buffer:
            _actions = sorted(self.action_buffer.values(), key=lambda ac: ac['_id'])
            _logs_block = self._sv_block_chain.gen_block(actions=self.action_logs)
            self.clean_up()
            return _actions, _logs_block
        return [], None


class SVBlockChain:
    def __init__(self, head):
        self.head = head
        self.prev = head
        self.current = head

    def gen_block(self, actions):
        svb = SVActionLogBlock(prev_block_hash=self.prev.id, actions=actions, serializer=BSONSerializer())
        self.prev, self.current = self.current, svb
        return svb
