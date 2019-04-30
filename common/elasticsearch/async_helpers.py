import asyncio
from operator import methodcaller
from elasticsearch_async import AsyncElasticsearch
from elasticsearch.helpers import expand_action
from elasticsearch.exceptions import *
from common import util

NO_RETRY_EXCEPTIONS = [SSLError, NotFoundError, AuthenticationException, AuthorizationException]


def _chunk_actions(actions, chunk_size, max_chunk_bytes, serializer):
    """
    Split actions into chunks by number or size, serialize them into strings in
    the process.
    """
    bulk_actions, bulk_data = [], []
    size, action_count = 0, 0
    for action, data in actions:
        raw_data, raw_action = data, action
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
        else:
            bulk_data.append((raw_action,))

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
            # todo: process error 429
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
            if attempted > max_retries:
                # if we are not propagating, mark all actions in current chunk as failed
                err_message = str(e)

                for data in bulk_data:
                    # collect all the information about failed actions
                    op_type, action = data[0].copy().popitem()
                    info = {"error": err_message, "status": 500, "create_time": util.utc_now()}
                    if op_type != 'delete':
                        info['data'] = data[1]
                    info['action'] = action
                    failed.append(info)
        else:
            to_retry, to_retry_data = [], []
            # go through request-response pairs and detect failures
            for (action, data), (ok, info) in zip(bulk_data, _chunk_result(bulk_data, result)):
                op, info = info.popitem()
                if not ok:
                    if attempted < max_retries:
                        to_retry.extend(map(client.transport.serializer.dumps, (action, data)))
                        to_retry_data.append((action, data))
                    else:
                        info = {
                            'error': str(info.get('error')),
                            'status': info.get('status'),
                            'action': action,
                            'data': data,
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

        delay = min(max_backoff, initial_backoff * 2 ** (attempted - 1))
        await asyncio.sleep(delay)

    return succeed, failed


async def bulk(client, actions, chunk_size=500, max_chunk_bytes=100 * 1024 * 1024,
               expand_action_callback=expand_action, max_retries=0, initial_backoff=2,
               max_backoff=600, **kwargs):
    """
    async helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
    a more human friendly interface - it consumes an iterator of actions and
    sends them to elasticsearch in chunks.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterable containing the actions to be executed
        ex: {'_op_type': 'index', '_index': 'rts_test', '_type': 'rt', '_id': 1,
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
    coros = []
    actions = map(expand_action_callback, actions)

    for bulk_data, bulk_actions in _chunk_actions(actions, chunk_size,
                                                  max_chunk_bytes,
                                                  client.transport.serializer):
        coro = _process_bulk_chunk(client,
                                   bulk_actions,
                                   bulk_data,
                                   max_retries=max_retries,
                                   initial_backoff=initial_backoff,
                                   max_backoff=max_backoff,
                                   **kwargs)
        coros.append(coro)

    done, _ = await asyncio.wait(coros)
    succeed, failed = [], []
    for future in done:
        s, f = future.result()
        succeed.extend(s)
        failed.extend(f)

    return succeed, failed
