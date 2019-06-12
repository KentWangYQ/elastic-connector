import sys
import bson.timestamp
import asyncio
import logging
import elasticsearch
import elasticsearch_async
from math import ceil
from datetime import datetime
from aiostream import stream
from common.elasticsearch import async_helpers
from common.elasticsearch.action_block import SVActionLogBlock, ActionLogBlockStatus, GENESIS_BLOCK
from common.elasticsearch.bson_serializer import BSONSerializer
from common import util
from . import constant
from .doc_manager_base import DocManagerBase
from .formatters import DefaultDocumentFormatter

logger = logging.getLogger('rts')


class ElasticOperate:
    index = 'index'
    update = 'update'
    delete = 'delete'


class DocManager(DocManagerBase):
    def __init__(self,
                 hosts,
                 loop=asyncio.get_event_loop(),
                 unique_key='_id',
                 chunk_size=constant.DEFAULT_CHUNK_SIZE,
                 log_index='mongo_log_block',
                 log_type='mongo_log_block',
                 error_index='mongo_error',
                 error_type='mongo_error',
                 attachment_field='content',
                 auto_commit=False,
                 auto_commit_interval=constant.DEFAULT_COMMIT_INTERVAL,
                 auto_monitor=True,
                 auto_monitor_interval=constant.DEFAULT_MONITOR_INTERVAL,
                 **kwargs):
        client_options = kwargs.get('client_options')
        if type(hosts) is not list:
            hosts = [hosts]

        self.loop = loop

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
            self.auto_committer.run()

        self.monitor = Monitor(auto_monitor_interval, loop=self.loop)
        if auto_monitor:
            self.monitor.run()

    @staticmethod
    def _index_and_mapping(namespace):
        """
        Namespace to index and doc_type
        :param namespace:
        :return:
        """
        index, doc_type = namespace.lower().split('.', 1)
        return index, doc_type

    def _gen_action(self, op_type, namespace, timestamp, doc, gen_log=True):
        action_log = {'ns': namespace,
                      'ts': timestamp,
                      'op': op_type,
                      'doc': doc} if gen_log else None

        index, doc_type = self._index_and_mapping(namespace)
        action = {
            '_op_type': op_type,
            '_index': index,
            '_type': doc_type
        }

        for key in async_helpers.ES_KEYS:
            if key in doc:
                action[key] = doc.pop(key)

        if '_i' in doc:
            action['_i'] = doc.pop('_i')

        # ObjectId to str
        for key in ('_parent', '_id'):
            if key in action and not isinstance(action[key], str):
                action[key] = str(action[key])

        action['_source'] = doc

        return action, action_log

    def _action_merge(self, pre_action, original_action):
        """
        Merge actions for same doc in the actions block

        Actions supported:
            - index
            - update
        :param pre_action:
        :param original_action:
        :return:
        """
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
                original_action['_source'] = {'doc': self.apply_update({}, original_action.get('_source'))}

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
        # if param doc_id gaven, use it as doc id
        if doc_id:
            doc['_id'] = doc_id

        # Get operate type, 'index' and 'update' was supported in this function
        _op_type = ElasticOperate.update if is_update else ElasticOperate.index
        # format doc
        doc = self._formatter.format_document(doc)
        # Generate the action and action log by doc info
        action, action_log = self._gen_action(_op_type, namespace, timestamp, doc)
        # Get actions for the doc in bulk buffer
        pre_action = self.bulk_buffer.get_action(doc_id)
        # Merge the pre_action in bulk buffer and current action
        action = self._action_merge(pre_action, action)
        # Push new action to bulk buffer
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
        logger.debug('Push action to buffer')

        # when bulk buffer has generated a block or commit interval is 0, commit immediately.
        if self.bulk_buffer.blocks or self.auto_commit_interval == 0:
            # commit
            self.auto_committer.skip_next()
            logger.debug('Skip next auto commit')
            self.commit()
            logger.debug('Commit, buffer reach the threshold')

    def index(self, doc, namespace, timestamp=util.utc_now()):
        """
        Index document
        :param doc:
        :param namespace:
        :param timestamp:
        :return:
        """
        self._upsert(doc, namespace, timestamp)
        logger.debug('[Index document] ns: %s' % namespace)

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
        logger.debug('[Update document] ns: %s' % namespace)

    def delete(self, doc, namespace, timestamp=util.utc_now()):
        """
        Delete document by doc_id
        :param doc:
        :param namespace:
        :param timestamp:
        :return:
        """
        action, action_log = self._gen_action(ElasticOperate.delete, namespace, timestamp, doc)
        self._push_to_buffer(action, action_log)
        logger.debug('[Delete document] ns: %s' % namespace)

    async def _chunk(self, actions, chunk_size, params):
        futures = []
        async with stream.chunks(actions, chunk_size).stream() as chunks:
            async for chunk in chunks:
                logger.debug('Elasticsearch bulk chunk size: %d' % len(chunk))
                for future in [future for future in futures if future.done()]:
                    # return all done future's result
                    yield future.result()
                    # remove future from futures list after yield result
                    futures.remove(future)

                logger.debug('Elasticsearch async helper bulk semaphore value: %d' % self.semaphore._value)
                await self.semaphore.acquire()
                future = async_helpers.bulk(client=self.es,
                                            actions=chunk,
                                            chunk_size=self.chunk_size,
                                            max_retries=3,
                                            initial_backoff=0.3,
                                            max_backoff=3,
                                            params=params,
                                            semaphore=self.semaphore)
                futures.append(asyncio.ensure_future(future))

            # await for all future complete
            if futures:
                done, _ = await asyncio.wait(futures)
                for future in done:
                    yield future.result()

    async def bulk_index(self, docs, namespace, params=None, chunk_size=None, doc_process=None, ):
        """
        Insert multiple documents into Elasticsearch directly.
        :return:
        """
        if not docs:
            return None

        if doc_process:
            docs = stream.map(docs, doc_process)

        docs = stream.map(docs, self._formatter.format_document)

        async def bulk(docs):
            succeed_total, failed_total = 0, 0
            async for (succeed, failed) in self._chunk(actions=docs, chunk_size=self.chunk_size, params=params):
                succeed_total += len(succeed)
                failed_total += len(failed)
                self.monitor.increase_succeed(len(succeed))
                self.monitor.increase_failed(len(failed))
                logger.info('[Direct bulk] ns:%s succeed:%d' % (namespace, len(succeed)))
                if failed:
                    logger.warning('[Direct bulk] ns:%s failed:%d' % (namespace, len(failed)))
                    _, failed = await asyncio.ensure_future(self._failed_actions_commit(failed))
                    if not failed:
                        logger.debug('Failed actions commit success')
                    else:
                        logger.warning('Failed actions commit failed')
            return succeed_total, failed_total

        return await bulk(stream.map(docs,
                                     lambda doc:
                                     self._gen_action(ElasticOperate.index, namespace, util.utc_now(), doc, False)[0]))

    async def _send_buffered_actions(self, action_buffer, action_log_block, refresh=False):
        """Send buffered actions to Elasticsearch"""
        # index log block
        succeed, failed = await asyncio.ensure_future(self._log_block_commit(action_log_block))
        if succeed:
            logger.debug('Log block commit success')
        else:
            logger.error('Log block commit error')
            raise Exception(failed)

        # bulk actions
        succeed, failed = await async_helpers.bulk(client=self.es, actions=action_buffer, max_retries=3,
                                                   initial_backoff=0.1,
                                                   max_backoff=1)
        self.monitor.increase_processed(len(action_buffer))
        if succeed:
            logger.info('[Bulk] succeed:%d' % len(succeed))
        if failed:
            logger.warning('[Bulk] failed:%d' % len(failed))

        if failed:
            _, failed = await asyncio.ensure_future(self._failed_actions_commit(failed))
            if not failed:
                logger.debug('Failed actions commit success')
            else:
                logger.warning('Failed actions commit failed')

        # commit log block result
        succeed, failed = await asyncio.ensure_future(self._log_block_done(action_log_block))
        if succeed:
            logger.debug('Mark log block done success')
        else:
            logger.warning('Mark log block done failed')

        if refresh:
            await self.es.indices.refresh()

    def commit(self, refresh=False):
        """Send bulk buffer to Elasticsearch, then refresh."""
        # send
        action_buffer, action_log_block = self.bulk_buffer.get_buffer()

        if action_buffer and action_log_block:
            asyncio.ensure_future(self._send_buffered_actions(action_buffer, action_log_block, refresh))
            logger.debug('Actions commit: %d' % len(action_buffer))

    def _log_block_commit(self, block):
        logger.debug('Log block commit')
        action, _ = self._gen_action(ElasticOperate.index, '.'.join([self.log_index, self.log_type]), util.utc_now(),
                                     block.to_dict(), gen_log=False)
        return async_helpers.bulk(client=self.es, actions=[action], max_retries=3,
                                  initial_backoff=0.1, max_backoff=1)

    def _log_block_done(self, block):
        logger.debug('Log block done')
        block_done_action = {
            '_op_type': ElasticOperate.update,
            '_index': self.log_index,
            '_type': self.log_type,
            '_id': block.id,
            '_source': {'doc': {
                'status': ActionLogBlockStatus.done,
                'actions': []
            }}
        }

        return async_helpers.bulk(client=self.es, actions=[block_done_action], max_retries=3, initial_backoff=0.1,
                                  max_backoff=1)

    def _failed_actions_commit(self, failed_actions):
        logger.debug('Failed actions commit')
        return async_helpers.bulk(client=self.es,
                                  actions=map(lambda action: {
                                      '_op_type': ElasticOperate.index,
                                      '_index': self.error_index,
                                      '_type': self.error_type,
                                      '_source': action
                                  }, failed_actions),
                                  max_retries=3,
                                  initial_backoff=0.1,
                                  max_backoff=1)

    async def stop(self, immediate=False):
        """Stop auto committer"""
        await self.auto_committer.stop()
        await self.monitor.stop()
        await self.es.transport.close()
        await asyncio.sleep(.25)

    def handle_command(self, command_doc, namespace, timestamp):
        raise NotImplementedError()

    def search(self):
        pass

    async def _wait_all_commit(self):
        """
        Commit all actions in buffer and wait complete

        help x_by_query to keep sequential continuity
        :return:
        """
        futures = []
        while self.bulk_buffer.has_action():
            futures.append(self.commit())
        if futures:
            await asyncio.wait(futures)

    async def delete_by_query(self, namespace, body, params=None):
        await self._wait_all_commit()  # commit all actions before this option, to keep sequential continuity
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
        self.run_future = None

    async def _run(self):
        while not self._stopped:
            if not self._skip_next:
                if self._auto_commit:
                    logger.debug('Auto commit')
                    self.docman.commit(refresh=True)
                elif self._auto_send:
                    logger.debug('Auto commit with no refresh')
                    self.docman.commit()
            self._skip_next = False
            await asyncio.sleep(self._sleep_interval)

    def run(self):
        self.run_future = asyncio.ensure_future(self._run(), loop=self.docman.loop)

    async def stop(self):
        if self._stopped:
            pass
        else:
            self._stopped = True
            logger.info('Auto committer is stopping...')
            if self.run_future:
                await self.run_future
            logger.info('Auto committer stopped')

    def skip_next(self):
        logger.debug('Skip next auto commit')
        self._skip_next = True


class Monitor:
    def __init__(self, interval=5, *, loop=asyncio.get_event_loop()):
        self.loop = loop
        self.processed = 0
        self.processed_total = 0
        self.succeed = 0
        self.succeed_total = 0
        self.failed = 0
        self.failed_total = 0

        self.interval = interval
        self._stopped = False
        self.run_future = None

    def _speed(self):
        start = datetime.now().timestamp()
        while True:
            self.processed_total += self.processed
            c, self.processed = self.processed, 0
            t = datetime.now().timestamp() - start
            start = datetime.now().timestamp()
            yield ceil(c / t)

    @staticmethod
    def _tasks_waiting():
        while True:
            yield len(asyncio.all_tasks())

    async def _run(self):
        s = self._speed()
        tw = self._tasks_waiting()
        while not self._stopped:
            logger.info(' ---- '.join(['[Tasks waiting] %d' % next(tw), '[Speed] %d items/sec' % next(s)]))
            await asyncio.sleep(self.interval)

    def run(self):
        self.run_future = asyncio.ensure_future(self._run(), loop=self.loop)

    async def stop(self):
        if self._stopped:
            pass
        else:
            self._stopped = True
            logger.info('Monitor is stopping...')
            if self.run_future:
                await self.run_future
            logger.info('Monitor stopped')

    def increase_processed(self, count):
        self.processed += count

    def increase_succeed(self, count):
        self.succeed += count
        self.increase_processed(count)

    def increase_failed(self, count):
        self.failed += count
        self.increase_processed(count)


class BulkBuffer:
    def __init__(self, docman, max_block_size):
        self.docman = docman  # doc manager
        self.max_block_size = max_block_size
        self.action_buffer = {}  # Action buffer for bulk indexing
        self.action_logs = []  # Action log for ES operation
        self.blocks = []
        self._i = -1  # order for action

    def global_counter(self):
        self._i += 1
        return self._i

    def has_action(self):
        return self.blocks or len(self.action_buffer) > 0

    def add_action(self, action, action_log):
        """
        兼容
        :param action:
        :param action_log:
        :return:
        """
        self.bulk_index(action, action_log)

    def get_action(self, _id):
        return self.action_buffer.get(str(_id))

    def bulk_index(self, action, action_log):
        if '_i' not in action:
            action['_i'] = self.global_counter()

        # depend on unique _id
        self.action_buffer[str(action.get('_id'))] = action
        self.action_logs.append(action_log)

        if len(self.action_buffer) >= self.max_block_size:
            self.blocks.append(self.gen_block(self.action_buffer.values(), self.action_logs))

    def reset_action(self, _id):
        self.action_buffer[_id] = {}

    def clean_up(self):
        self.action_buffer = {}
        self.action_logs = []

    def get_buffer(self):
        if self.blocks:
            return self.get_block()
        if self.action_buffer:
            return self.gen_block(self.action_buffer.values(), self.action_logs)
        return None, None

    def gen_block(self, actions, action_logs):
        _actions = sorted(actions, key=lambda ac: ac['_id'])
        _logs_block = self.docman.log_block_chain.gen_block(actions=action_logs)
        self.clean_up()
        return _actions, _logs_block

    def get_block(self):
        if self.blocks:
            return self.blocks.pop(0)
        return [], None


class BlockChain:
    __TS_MARK_FILE = sys.path[0] + '/ts_mark.data'
    __TS_MARK_FILE_DEBUG = sys.path[1] + '/ts_mark.data'

    def __init__(self, docman: DocManager, serializer=BSONSerializer()):
        self.docman = docman
        self.serializer = serializer

        block = self._get_last_block()
        if not block:
            self._commit_genesis_block()
            block = GENESIS_BLOCK
        self.head, self.prev, self.current = (block,) * 3

        self.last_ts = self.current.last_action.get('ts')

    def gen_block(self, actions):
        # The create_time of log block can be the order of blocks
        svb = SVActionLogBlock(prev_block_hash=self.prev.id, actions=actions, serializer=self.serializer)
        self.prev, self.current = self.current, svb
        return svb

    def get_processing_block(self):
        body = {
            "size": 10000,
            "_source": [
                "first_action.ts",
                "last_action.ts"
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
                    "create_time": {
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
        block = None
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
                        "create_time": {
                            "order": "asc"
                        }
                    }
                ]
            }

            rn = self.docman.es_sync.search(index=self.docman.log_index, doc_type=self.docman.log_type,
                                            body=first_non_valid_block_query_body)
            t = util.utc_now()
            if rn.get('hits').get('total') > 0:
                t = rn.get('hits').get('hits')[0].get('_source').get('create_time') or t

            self._clean_invalid_block(t)
            last_valid_block_query_body = {
                "size": 1,
                "sort": [
                    {
                        "create_time": {
                            "order": "desc"
                        }
                    }
                ]
            }
            rl = self.docman.es_sync.search(index=self.docman.log_index, doc_type=self.docman.log_type,
                                            body=last_valid_block_query_body)
            if rl.get('hits').get('total') > 0:
                block = self._gen_sv_block_from_dict(rl.get('hits').get('hits')[0].get('_source'))

        if not block:
            logger.info('Last block does NOT exist')
        else:
            logger.info('Get last block success')

        return block

    def _clean_invalid_block(self, start_time):
        self.docman.es_sync.delete_by_query(index=self.docman.log_index,
                                            doc_type=self.docman.log_type,
                                            body={
                                                "query": {
                                                    "range": {
                                                        "create_time": {
                                                            "gte": start_time
                                                        }
                                                    }
                                                }
                                            })
        self.docman.es_sync.indices.refresh()
        logger.info('Invalid log block was cleaned')

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

    def clear(self):
        """
        Clear the log black chain
        :return:
        """
        self.docman.delete_index('.'.join([self.docman.log_index, self.docman.log_type]),
                                 params={'ignore_unavailable': True})
        logger.info('Log block chain cleared')

    def _commit_genesis_block(self):
        # current timestamp as last ts
        GENESIS_BLOCK.last_action = {'ts': bson.timestamp.Timestamp(util.now_timestamp_s(), 1)}
        # commit genesis block
        body = GENESIS_BLOCK.to_dict()
        _id = body.pop('_id')
        self.docman.es_sync.index(index=self.docman.log_index, doc_type=self.docman.log_type, body=body, id=_id)
        logger.info('GENESIS BLOCK committed')
