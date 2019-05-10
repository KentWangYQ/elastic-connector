import os
import sys
import pickle
import bson.timestamp
import asyncio
import elasticsearch
import elasticsearch_async
from math import ceil
from datetime import datetime
from aiostream import stream
from common.elasticsearch import async_helpers
from common.elasticsearch.action_log import SVActionLogBlock, ActionLogBlockStatus, GENESIS_BLOCK
from common.elasticsearch.bson_serializer import BSONSerializer
from common import util
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
                 chunk_size=constant.DEFAULT_CHUNK_SIZE,
                 log_index='mongodb_log_block',
                 log_type='mongodb_log_block',
                 error_index='mongodb_error',
                 error_type='mongodb_error',
                 attachment_field='content',
                 auto_commit=False,
                 **kwargs):
        client_options = kwargs.get('client_options')
        if type(hosts) is not list:
            hosts = [hosts]
        self.es_sync = elasticsearch.Elasticsearch(hosts=hosts, **client_options)
        self.es = elasticsearch_async.AsyncElasticsearch(hosts=hosts, **client_options)
        self._formatter = DefaultDocumentFormatter()
        self.chunk_size = chunk_size

        # used for elasticsearch request traffic limit
        self.semaphore = asyncio.Semaphore(kwargs.get('semaphore_value') or constant.CONCURRENT_LIMIT)

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
        self.attachment_field = attachment_field

        self.log_block_chain = BlockChain(self)

        self.bulk_buffer = BulkBuffer(self, self.chunk_size)

        self.auto_committer = AutoCommitter(self, self.auto_send_interval, self.auto_commit_interval)
        if auto_commit:
            asyncio.ensure_future(self.auto_committer.run())

        self._processed = 0  # 处理完成条目技术，用于速度计算

    @staticmethod
    def _index_and_mapping(namespace):
        """
        Namespace to index and doc_type
        :param namespace:
        :return:
        """
        index, doc_type = namespace.lower().split('.', 1)
        return index, doc_type

    def _action_merge(self, pre_action, original_action):
        if pre_action:
            # 处理_op_type:
            # 1. (pre_op_type, _original_op_type)存在'index'，则为'index';
            # 2. 否则为'update'.
            p_op_type = pre_action.get('_op_type')
            o_op_type = original_action.get('_op_type')
            _op_type = ElasticOperate.index if (ElasticOperate.index in (p_op_type, o_op_type)) else o_op_type
            p_doc = pre_action.get('_source', {}).get('doc', {}) \
                if p_op_type == ElasticOperate.update else pre_action.get('_source', {})
            o_doc = original_action.get('_source', {})
            # 合并doc
            _source = self.apply_update(p_doc, o_doc)
            _source = {'doc': _source} if _op_type == ElasticOperate.update else _source

            original_action['_op_type'] = _op_type
            original_action['_source'] = _source
        else:
            if original_action.get('_op_type') == ElasticOperate.update:
                original_action['_source'] = {'doc': original_action.get('_source')}

        return original_action

    def _upsert(self, doc, namespace, timestamp=util.utc_now(), *, doc_id=None, is_update=False):
        """
        index or update document
        :param doc: native object
        :param namespace:
        :param timestamp:
        :param is_update:
        :return:
        """
        index, doc_type = self._index_and_mapping(namespace)
        _o_id = doc.pop('_id', '')
        _parent = str(doc.pop('_parent', ''))

        doc_id = str(doc_id or _o_id)
        doc['doc_id'] = doc_id

        _original_op_type = ElasticOperate.update if is_update else ElasticOperate.index
        _op_type = _original_op_type

        doc = self._formatter.format_document(doc)

        pre_action = self.bulk_buffer.get_action(doc_id)

        action = self._action_merge(pre_action, {
            '_op_type': _op_type,
            '_index': index,
            '_type': doc_type,
            '_id': doc_id,
            '_source': doc
        })

        if _parent:
            action['_parent'] = _parent

        action_log = {**{'ns': namespace,
                         'ts': timestamp,
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
        if self.bulk_buffer.blocks or self.auto_commit_interval == 0:
            # commit
            self.auto_committer.skip_next()
            self.commit()

    def index(self, doc, namespace, timestamp=util.utc_now()):
        """
        Index document
        :param doc:
        :param namespace:
        :param timestamp:
        :return:
        """
        self._upsert(doc, namespace, timestamp)

    def update(self, doc_id, doc, namespace, timestamp=util.utc_now()):
        """
        Update document
        :param doc_id:
        :param doc:
        :param namespace:
        :param timestamp:
        :return:
        """
        self._upsert(doc, namespace, timestamp, doc_id=doc_id, is_update=True)

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
            'ts': timestamp,
            'op': _op_type,
            'doc_id': doc_id
        }
        self._push_to_buffer(action, action_log)

    async def bulk_index(self, docs, namespace, params=None, chunk_size=None):
        """
        Insert multiple documents into Elasticsearch directly.
        :return:
        """
        if not docs:
            return None
        index, doc_type = self._index_and_mapping(namespace)

        def dm(doc):
            # async for doc in docs:
            _parent = str(doc.pop('_parent', ''))
            action = {
                '_op_type': ElasticOperate.index,
                '_index': index,
                '_type': doc_type,
                '_id': str(doc.pop('_id', '')),
                '_source': doc
            }
            if _parent:
                action['_parent'] = _parent
            return action

        # async with stream.chunks(docs, chunk_size or self.chunk_size).stream() as chunks:

        async def t(chunk):  # todo 重构\
            async for (succeed, failed) in async_helpers.bulk(client=self.es,
                                                              actions=chunk,  # todo: 实际读取后置
                                                              max_retries=3,
                                                              initial_backoff=0.1,
                                                              max_backoff=1,
                                                              params=params,
                                                              expand_action_callback=dm,
                                                              semaphore=self.semaphore):
                # print(result)
                print('succeed:', len(succeed), 'failed:', len(failed))

        # async for chunk in stream.chunks(docs, chunk_size or self.chunk_size).stream():
        #     print(1)
        #     future = asyncio.ensure_future(t(chunk))
        #     await asyncio.wait([future])
        #     await asyncio.wait([future])

        # async with stream.chunks(docs, chunk_size or self.chunk_size).stream() as chunks:
        #     async for (succeed, failed) in asyncio.as_completed(
        #             [async_helpers.bulk(client=self.es,
        #                                 actions=map(dm, chunk),  # todo: 实际读取后置
        #                                 max_retries=3,
        #                                 initial_backoff=0.1,
        #                                 max_backoff=1,
        #                                 params=params,
        #                                 semaphore=self.semaphore) async for chunk in chunks]):
        #         self._processed += len(succeed) + len(failed)
        #         print('succeed:', len(succeed), 'failed:', len(failed))
        #
        asyncio.ensure_future(t(docs))

    async def _send_buffered_actions(self, action_buffer, action_log_block, refresh=False):
        """Send buffered actions to Elasticsearch"""
        # future for index log block
        logs_future = self._send_action_log_block(action_log_block)
        # future for bulk actions
        actions_future = asyncio.ensure_future(
            async_helpers.bulk(client=self.es, actions=action_buffer, max_retries=3,
                               initial_backoff=0.1,
                               max_backoff=1))
        # wait for futures complete
        await asyncio.wait([logs_future, actions_future])

        # bulk result which is a tuple for succeed and failed actions: (succeed, failed)
        succeed, failed = actions_future.result()
        self._processed += len(succeed) + len(failed)
        print('succeed:', len(succeed), 'failed:', len(failed))
        # print(succeed, failed)

        # commit log block result
        await self._commit_log(action_log_block, failed)
        if refresh:
            await self.es.indices.refresh()

    def send_buffered_actions(self, refresh=False):
        # get action buffer and operate log block
        action_buffer, action_log_block = self.bulk_buffer.get_buffer()

        if action_buffer and action_log_block:
            asyncio.ensure_future(self._send_buffered_actions(action_buffer, action_log_block, refresh))

    def _send_action_log_block(self, block):
        action_log = {
            '_op_type': ElasticOperate.index,
            '_index': self.log_index,
            '_type': self.log_type,
            '_id': block.id,
            '_source': block.to_dict()
        }
        return asyncio.ensure_future(async_helpers.bulk(client=self.es, actions=[action_log], max_retries=3,
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
            '_source': {'doc': {
                'status': ActionLogBlockStatus.done,
                'actions': []
            }}
        }
        actions = [log_commit]
        actions.extend(map(_emf, failed))
        return asyncio.ensure_future(async_helpers.bulk(client=self.es, actions=actions, max_retries=3,
                                                        initial_backoff=0.1,
                                                        max_backoff=1))

    def commit(self):
        """Send bulk buffer to Elasticsearch, then refresh."""
        # send
        self.send_buffered_actions(refresh=True)

    async def stop(self):
        """Stop auto committer"""
        # todo: 完整处理stop，定义scope
        self.auto_committer.stop()
        await asyncio.sleep(1)
        # all_tasks = asyncio.all_tasks(asyncio.get_event_loop())
        # asyncio.ensure_future(asyncio.wait(all_tasks))
        await self.es.transport.close()
        await asyncio.sleep(0.25)

    def handle_command(self, command_doc, namespace, timestamp):
        raise NotImplementedError()

    def search(self):
        pass

    def delete_by_query_sync(self, namespace, body, params=None):
        index, doc_type = self._index_and_mapping(namespace)
        params = params or {}
        return self.es_sync.delete_by_query(index=index, body=body, doc_type=doc_type, params=params)

    def delete_index(self, namespace, params=None):
        index, _ = self._index_and_mapping(namespace)
        params = params or {}
        return self.es_sync.indices.delete(index=index, params=params)


class AutoCommitter:
    def __init__(self, docman, send_interval=0, commit_interval=0, sleep_interval=5):
        self.docman = docman
        self._send_interval = send_interval
        self._commit_interval = commit_interval
        self._auto_send = self._send_interval > 0
        self._auto_commit = self._commit_interval > 0
        self._sleep_interval = sleep_interval
        self._stopped = False
        self._skip_next = False

    async def run(self):
        p = self.ping()
        while not self._stopped:
            if not self._skip_next:
                if self._auto_commit:
                    self.docman.commit()

                elif self._auto_send:
                    self.docman.send_buffered_actions()
            self._skip_next = False
            try:
                await asyncio.sleep(self._sleep_interval)
                print(next(p), end='')
            except Exception as e:
                print(e)

    def stop(self):
        self._stopped = True

    def skip_next(self):
        self._skip_next = True

    def ping(self):
        # todo: 独立成Monitor，监视器
        start = 0
        while not self._stopped:
            # for i in range(10):
            #     yield '.'
            # yield '\n'
            c, self.docman._processed = self.docman._processed, 0
            t = datetime.now().timestamp() - start
            start = datetime.now().timestamp()

            if start == 0:
                yield ''

            yield 'tasks waiting: %s --- speed: %d items/sec\n' % (
                len(asyncio.all_tasks()), ceil(c / t))


class BulkBuffer:
    def __init__(self, docman, max_block_size):
        self.docman = docman  # doc manager
        self.max_block_size = max_block_size
        self.action_buffer = {}  # Action buffer for bulk indexing
        self.action_logs = []  # Action log for ES operation
        self.blocks = []
        self._i = -1  # priority for action

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

        # depend on unique _id
        self.action_buffer[str(action.get('_id'))] = action
        self.action_logs.append(action_log)

        if self.count() >= self.max_block_size:
            self.blocks.append(self.get_buffer())

    def reset_action(self, _id):
        self.action_buffer[_id] = {}

    def clean_up(self):
        self.action_buffer = {}
        self.action_logs = []

    def get_buffer(self):
        if self.blocks:
            return self.get_block()
        if self.action_buffer:
            _actions = sorted(self.action_buffer.values(), key=lambda ac: ac['_id'])
            _logs_block = self.docman.log_block_chain.gen_block(actions=self.action_logs)
            self.clean_up()
            return _actions, _logs_block
        return [], None

    def get_block(self):
        if self.blocks:
            return self.blocks.pop(0)
        return [], None


class BlockChain:
    __TS_MARK_FILE = sys.path[1] + '/ts_mark.data'

    # todo 增加清理早期block
    def __init__(self, docman: DocManager, serializer=BSONSerializer()):
        self.docman = docman
        self.serializer = serializer
        block = self._get_last_block()
        self.head, self.prev, self.current = (block,) * 3
        self.last_ts = self._get_last_ts_mark() or self.current.last_action.get('ts') or bson.timestamp.Timestamp(
            util.now_timestamp_s(), 1)

    def mark_ts(self):
        """
        Mark current timestamp
        Use as last ts in some scenes
            - Index all indices
            - Reindex one or more indices
        :return:
        """
        f = open(self.__TS_MARK_FILE, 'wb')
        pickle.dump(bson.timestamp.Timestamp(util.now_timestamp_s(), 1), f)
        f.close()

    def _get_last_ts_mark(self):
        """
        Get last ts mark.

        If mark file not exist or read failed, return None
        :return:
        """
        x = None
        if os.path.exists(self.__TS_MARK_FILE):
            try:
                with open(self.__TS_MARK_FILE, 'rb') as f:
                    x = pickle.load(f)
            except Exception as e:
                # todo: 增加错误处理，WARNING
                pass

            try:
                os.remove(self.__TS_MARK_FILE)
            except Exception as e:
                # todo: 增加错误处理，WARNING
                pass

        return x

    def gen_block(self, actions):
        # The create_time of log block can be the order of blocks
        svb = SVActionLogBlock(prev_block_hash=self.prev.id, actions=actions, serializer=self.serializer)
        self.prev, self.current = self.current, svb
        return svb

    def get_processing_block(self):
        body = {
            "size": 10000,
            "_source": [
                "first_action.ts.$date",
                "last_action.ts.$date"
            ],
            "query": {
                "term": {
                    "status.keyword": {
                        "value": ActionLogBlockStatus.processing
                    }
                }
            },
            "sort": [
                {
                    "create_time.$date": {
                        "order": "desc"
                    }
                }
            ]
        }
        result = self.docman.es_sync.search(index=self.docman.log_index, doc_type=self.docman.log_type, body=body)
        blocks = []
        for r in result.get('hits').get('hits'):
            blocks.append(self._gen_sv_block_from_dict(r.get('_source')))

        return blocks

    def _get_last_block(self):

        block = GENESIS_BLOCK
        if self.docman.es_sync.indices.exists_type(index=self.docman.log_index,
                                                   doc_type=self.docman.log_type):

            first_non_valid_block_query_body = {
                "size": 1,
                "query": {
                    "term": {
                        "status.keyword": {
                            "value": ActionLogBlockStatus.processing
                        }
                    }
                },
                "sort": [
                    {
                        "create_time.$date": {
                            "order": "asc"
                        }
                    }
                ]
            }

            rn = self.docman.es_sync.search(index=self.docman.log_index, doc_type=self.docman.log_type,
                                            body=first_non_valid_block_query_body)
            t = util.utc_now_str()
            if rn.get('hits').get('total') > 0:
                t = rn.get('hits').get('hits')[0].get('_source').get('create_time', {}).get('$date') or t

            last_valid_block_query_body = {
                "size": 1,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "status.keyword": {
                                        "value": ActionLogBlockStatus.done
                                    }
                                }
                            },
                            {
                                "range": {
                                    "create_time.$date": {
                                        "lt": t
                                    }
                                }
                            }
                        ]
                    }
                },
                "sort": [
                    {
                        "create_time.$date": {
                            "order": "desc"
                        }
                    }
                ]
            }
            rl = self.docman.es_sync.search(index=self.docman.log_index, doc_type=self.docman.log_type,
                                            body=last_valid_block_query_body)
            if rl.get('hits').get('total') > 0:
                block = self._gen_sv_block_from_dict(
                    rl.get('hits').get('hits')[0].get('_source')) or t
        return block

    def _gen_sv_block_from_dict(self, obj):
        block = SVActionLogBlock(prev_block_hash=obj.get('prev_block_hash'),
                                 actions=obj.get('actions'),
                                 create_time=obj.get('create_time'),
                                 status=obj.get('status'),
                                 serializer=self.serializer)
        if 'id' in obj:
            block.id = obj.get('id')
        if 'merkle_root_hash' in obj:
            block.merkle_root_hash = obj.get('merkle_root_hash')
        if 'actions_count' in obj:
            block.actions_count = obj.get('actions_count')
        if 'first_action' in obj:
            block.first_action = obj.get('first_action')
        if 'last_action' in obj:
            block.last_action = obj.get('last_action')

        return block
