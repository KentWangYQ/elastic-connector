import logging
import asyncio
from operator import methodcaller
from elasticsearch_async import AsyncElasticsearch
from elasticsearch.exceptions import *
from common import util

DEFAULT_CHUNK_SIZE = 2000
DEFAULT_CHUNK_BYTES = 100 * 1024 * 1024
NO_RETRY_EXCEPTIONS = [SSLError, NotFoundError, AuthenticationException, AuthorizationException]
STRING_TYPES = str, bytes
logger = logging.getLogger('rts')

ES_KEYS = ('_index', '_parent', '_percolate', '_routing', '_timestamp',
           '_type', '_version', '_version_type', '_id',
           '_retry_on_conflict', 'pipeline')


def expand_action(data):
    """
    From one document or action definition passed in by the user extract the
    action/data lines needed for elasticsearch's
    :meth:`~elasticsearch.Elasticsearch.bulk` api.
    """
    # when given a string, assume user wants to index raw json
    if isinstance(data, STRING_TYPES):
        return '{"index":{}}', data

    # make sure we don't alter the action
    data = data.copy()
    op_type = data.pop('_op_type', 'index')
    action = {op_type: {}}
    for key in ES_KEYS:
        if key in data and data.get(key) is not None:
            action[op_type][key] = data.pop(key)

    # no data payload for delete
    if op_type == 'delete':
        return action, None

    return action, data.get('_source', data)


def _chunk_actions(docs, chunk_size, max_chunk_bytes, serializer):
    """
    Split actions into chunks by number or size, serialize them into strings in
    the process.
    """
    bulk_actions, bulk_data = [], []
    size, action_count = 0, 0
    for action, data in docs:
        raw_action, raw_data = action, data
        action = serializer.dumps(action)
        cur_size = len(action) + 1

        if data is not None:
            data = serializer.dumps(data)
            cur_size += len(data) + 1

        # full chunk, send it and start a new one
        if bulk_actions and (size + cur_size > max_chunk_bytes or action_count == chunk_size):
            yield bulk_data, bulk_actions
            bulk_actions, bulk_data = [], []
            size, action_count = 0, 0

        bulk_actions.append(action)
        if data is not None:
            bulk_actions.append(data)

        bulk_data.append((raw_action, raw_data))

        size += cur_size
        action_count += 1

    if bulk_actions:
        yield bulk_data, bulk_actions


def _chunk_result(_bulk_data, _result):
    """
    Streaming process elasticsearch request result.
    :param _bulk_data:
    :param _result:
    :return:
    """
    for _data, (_op_type, item) in zip(_bulk_data, map(methodcaller('popitem'), _result.get('items', []))):
        _ok = 200 <= item.get('status', 500) < 300
        yield (_ok, {_op_type: item})


async def _process_bulk_chunk(client: AsyncElasticsearch,
                              bulk_actions,
                              bulk_data,
                              max_retries,
                              initial_backoff,
                              max_backoff,
                              **kwargs):
    """
    Send a bulk request to elasticsearch and process the output, it will retry when exception raised.
    """
    attempted = 0
    succeed, failed = [], []
    while attempted <= max_retries:
        # send the actual request
        future = client.bulk('\n'.join(bulk_actions) + '\n', **kwargs)
        attempted += 1

        # if raise on error is set, we need to collect errors per chunk before raising them
        try:
            result = await future
        except TransportError as e:
            logger.warning('[Elasticsearch] %r', e)
            if type(e) in NO_RETRY_EXCEPTIONS or attempted > max_retries:
                # if we are not propagating, mark all actions in current chunk as failed
                err_message = str(e)

                for data in bulk_data:
                    # collect all the information about failed actions
                    op_type, action = data[0].copy().popitem()
                    info = {"error": err_message, "status": e.status_code, "create_time": util.utc_now()}
                    if op_type != 'delete':
                        info['data'] = data[1]
                    info['action'] = action
                    failed.append(info)
        except Exception as e:
            logger.warning('[AsyncHelper] %r', e)
            if attempted > max_retries:
                # if we are not propagating, mark all actions in current chunk as failed
                err_message = str(e)

                for data in bulk_data:
                    # collect all the information about failed actions
                    op_type, action = data[0].copy().popitem()
                    info = {"error": err_message, "status": 500, "create_time": util.utc_now(), "action": action}
                    # if op_type != 'delete':
                    #     info['data'] = data[1]
                    failed.append(info)
        else:
            to_retry, to_retry_data = [], []
            # go through request-response pairs and detect failures
            for (action, data), (ok, info) in zip(bulk_data, _chunk_result(bulk_data, result)):
                op, info = info.popitem()
                if not ok and info.get('status') != 404:
                    if attempted < max_retries:
                        to_retry.append(client.transport.serializer.dumps(action))
                        if data:
                            to_retry.append(client.transport.serializer.dumps(data))
                        to_retry_data.append((action, data))
                    else:
                        info = {
                            'error': str(info.get('error')),
                            'status': info.get('status'),
                            'action': action,
                            # 'data': data,
                            'create_time': util.utc_now()
                        }
                        failed.append(info)
                else:
                    # succeed or max retry
                    succeed.append(info)

                # retry only subset of documents that didn't succeed
                if attempted < max_retries:
                    bulk_actions, bulk_data = to_retry, to_retry_data

            if not to_retry:
                # all success, no need to retry
                break
        finally:
            delay = min(max_backoff, initial_backoff * 2 ** (attempted - 1))
            await asyncio.sleep(delay)
            if attempted <= max_retries:
                logger.debug('Elasticsearch bulk request retry')

    return succeed, failed


async def bulk(client, actions, chunk_size=DEFAULT_CHUNK_SIZE, max_chunk_bytes=DEFAULT_CHUNK_BYTES,
               expand_action=expand_action, max_retries=0, initial_backoff=2,
               max_backoff=600, semaphore=None, **kwargs):
    """
    async helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
    a more human friendly interface - it consumes an iterator of actions and
    sends them to elasticsearch in chunks.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterable containing the actions to be executed
        ex: {'_op_type': 'index', '_index': 'rts_test', '_type': 'rt', '_id': 1, '_parent':2,
            _source:{'now': datetime.datetime.now()}},
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
    :arg expand_action_callback: callback executed on each action passed in,
        should return a tuple containing the action line and the data line
        (`None` if data line should be omitted).
    :arg max_retries: maximum number of times a document will be retried when
        ``429`` is received, set to 0 (default) for no retires on ``429``
    :arg initial_backoff: number of seconds we should wait before the first
        retry. Any subsequent retries will be powers of ``inittial_backoff *
        2**retry_number``
    :arg max_backoff: maximum number of seconds a retry will wait
    """
    actions = map(expand_action, actions)

    succeed, failed = [], []

    for bulk_data, bulk_actions in _chunk_actions(actions, chunk_size,
                                                  max_chunk_bytes,
                                                  client.transport.serializer):
        s, f = await _process_bulk_chunk(client,
                                         bulk_actions,
                                         bulk_data,
                                         max_retries=max_retries,
                                         initial_backoff=initial_backoff,
                                         max_backoff=max_backoff,
                                         **kwargs)
        succeed.extend(s)
        failed.extend(f)
    if semaphore:
        # release semaphore
        assert isinstance(semaphore, asyncio.Semaphore)
        semaphore.release()
    return succeed, failed
