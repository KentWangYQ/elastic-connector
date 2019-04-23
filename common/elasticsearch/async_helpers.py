import asyncio
import functools
from elasticsearch_async import AsyncElasticsearch
from elasticsearch.helpers import *
from elasticsearch.exceptions import *

RETRY_EXCEPTIONS = [ConnectionError, ConnectionTimeout, ConflictError, RequestError]


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


def _process_bulk_chunk(client: AsyncElasticsearch,
                        bulk_actions,
                        bulk_data,
                        max_retries,
                        initial_backoff,
                        max_backoff,
                        attempted=0,
                        done_callback=None,
                        **kwargs):
    """
    Send a bulk request to elasticsearch and process the output.
    """
    future = client.bulk('\n'.join(bulk_actions) + '\n', **kwargs)

    future.add_done_callback(functools.partial(_process_bulk_result,
                                               client=client,
                                               bulk_actions=bulk_actions,
                                               bulk_data=bulk_data,
                                               max_retries=max_retries,
                                               initial_backoff=initial_backoff,
                                               max_backoff=max_backoff,
                                               attempted=attempted,
                                               done_callback=done_callback,
                                               **kwargs))

    return future

    # attempted = 0
    # while attempted <= max_retries:
    #     # send the actual request
    #     future = client.bulk('\n'.join(bulk_actions) + '\n', **kwargs)
    #     attempted += 1
    #
    #     # if raise on error is set, we need to collect errors per chunk before raising them
    #     try:
    #         result = yield from future
    #     except TransportError as e:
    #         if type(e) not in RETRY_EXCEPTIONS or attempted >= max_retries:
    #             # todo: 收集failed
    #             # if we are not propagating, mark all actions in current chunk as failed
    #             err_message = str(e)
    #
    #             for data in bulk_data:
    #                 # collect all the information about failed actions
    #                 op_type, action = data[0].copy().popitem()
    #                 info = {"error": err_message, "status": e.status_code, "exception": e}
    #                 if op_type != 'delete':
    #                     info['data'] = data[1]
    #                 info.update(action)
    #                 yield (False, info)  # todo 与else中的返回保持一致
    #     else:
    #         # go through request-response pairs and detect failures
    #         def _chunk_result(_bulk_data, _result):
    #             for _data, (_op_type, item) in zip(_bulk_data, map(methodcaller('popitem'), _result.get('items', []))):
    #                 _ok = 200 <= item.get('status', 500) < 300
    #                 yield _ok, {_op_type: item}
    #
    #         # region retry
    #         to_retry, to_retry_data = [], []
    #
    #         for data, (ok, info) in zip(bulk_data, _chunk_result(bulk_data, result)):
    #             action, info = info.popitem()
    #             if not ok and attempted < max_retries:
    #                 # _process_bulk_chunk expects strings so we need to
    #                 # re-serialize the data
    #                 to_retry.extend(map(client.transport.serializer.dumps, data))
    #                 to_retry_data.append(data)
    #             else:
    #                 # succeed or max retry
    #                 yield (action, info)  # todo: 确认记录所需的信息
    #
    #             # retry only subset of documents that didn't succeed
    #             if attempted < max_retries:
    #                 delay = min(max_backoff, initial_backoff * 2 ** (attempted - 1))
    #                 yield from asyncio.sleep(delay)
    #                 bulk_actions, bulk_data = to_retry, to_retry_data
    #         # endregion


# region _process_bulk_result
def _process_bulk_result(future,
                               client: AsyncElasticsearch,
                               bulk_actions,
                               bulk_data,
                               max_retries,
                               initial_backoff,
                               max_backoff,
                               attempted,
                               done_callback=None):
    succeed, failed, result = [], [], {}
    # if raise on error is set, we need to collect errors per chunk before raising them
    if future.cancelled():
        pass  # todo: cancel 处理
    else:
        try:
            result = future.result()
        except TransportError as e:
            if attempted <= max_retries:
                if type(e) in (ConnectionError, ConnectionTimeout, ConflictError, RequestError):
                    # request failed retry
                    _process_bulk_chunk(client, bulk_actions, bulk_data, max_retries, initial_backoff, max_backoff,
                                        attempted + 1, done_callback)
                    return

            # if we are not propagating, mark all actions in current chunk as failed
            err_message = str(e)

            for data in bulk_data:
                # collect all the information about failed actions
                op_type, action = data[0].copy().popitem()
                info = {"error": err_message, "status": e.status_code, "exception": e}
                if op_type != 'delete':
                    info['data'] = data[1]
                info.update(action)
                failed.append({op_type: info})
        else:
            # go through request-response pairs and detect failures
            def _chunk_result(_bulk_data, _result):
                for _data, (_op_type, item) in zip(_bulk_data, map(methodcaller('popitem'), _result['items'])):
                    _ok = 200 <= item.get('status', 500) < 300
                    yield _ok, {_op_type: item}

            # region retry
            if attempted <= max_retries:
                to_retry, to_retry_data = [], []

                for data, (ok, info) in zip(bulk_data, _chunk_result(bulk_data, result)):
                    action, info = info.popitem()
                    if not ok:
                        # _process_bulk_chunk expects strings so we need to
                        # re-serialize the data
                        to_retry.extend(map(client.transport.serializer.dumps, data))
                        to_retry_data.append(data)
                    else:
                        succeed.append((action, info))  # todo: 确认成功记录所需的信息

                # retry only subset of documents that didn't succeed
                delay = min(max_backoff, initial_backoff * 2 ** (attempted - 1))
                asyncio.sleep(delay)
                asyncio.get_event_loop().call_later(delay,
                                                    _process_bulk_chunk,
                                                    client,
                                                    to_retry,
                                                    to_retry_data,
                                                    max_retries,
                                                    initial_backoff,
                                                    max_backoff,
                                                    attempted + 1,
                                                    done_callback)
            # endregion

        if done_callback:
            done_callback(future, succeed, failed)  # todo: 处理成功结果，当前计划记录成功操作，并持久化到ES


# endregion


def streaming_bulk(client, actions, chunk_size=500, max_chunk_bytes=100 * 1024 * 1024,
                   expand_action_callback=expand_action, max_retries=0, initial_backoff=2,
                   max_backoff=600, done_callback=None, **kwargs):
    """
    Streaming bulk consumes actions from the iterable passed in and yields
    results per action. For non-streaming usecases use
    :func:`~elasticsearch.helpers.bulk` which is a wrapper around streaming
    bulk that returns summary information about the bulk operation once the
    entire input is consumed and sent.

    If you specify ``max_retries`` it will also retry any documents that were
    rejected with a ``429`` status code. To do this it will wait (**by calling
    time.sleep which will block**) for ``initial_backoff`` seconds and then,
    every subsequent rejection for the same chunk, for double the time every
    time up to ``max_backoff`` seconds.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterable containing the actions to be executed
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
    :arg raise_on_error: raise ``BulkIndexError`` containing errors (as `.errors`)
        from the execution of the last chunk when some occur. By default we raise.
    :arg raise_on_exception: if ``False`` then don't propagate exceptions from
        call to ``bulk`` and just report the items that failed as failed.
    :arg expand_action_callback: callback executed on each action passed in,
        should return a tuple containing the action line and the data line
        (`None` if data line should be omitted).
    :arg max_retries: maximum number of times a document will be retried when
        ``429`` is received, set to 0 (default) for no retires on ``429``
    :arg initial_backoff: number of seconds we should wait before the first
        retry. Any subsequent retries will be powers of ``inittial_backoff *
        2**retry_number``
    :arg max_backoff: maximum number of seconds a retry will wait
    :arg yield_ok: if set to False will skip succeedful documents in the output
    """
    futures = []
    actions = map(expand_action_callback, actions)

    for bulk_data, bulk_actions in _chunk_actions(actions, chunk_size,
                                                  max_chunk_bytes,
                                                  client.transport.serializer):
        future = _process_bulk_chunk(client,
                                     bulk_actions,
                                     bulk_data,
                                     max_retries=max_retries,
                                     initial_backoff=initial_backoff,
                                     max_backoff=max_backoff,
                                     done_callback=done_callback,
                                     **kwargs)
        futures.append(future)

    return futures


# todo: 异步改造
def bulk(client, actions, stats_only=False, **kwargs):
    """
    Helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
    a more human friendly interface - it consumes an iterator of actions and
    sends them to elasticsearch in chunks. It returns a tuple with summary
    information - number of successfully executed actions and either list of
    errors or number of errors if ``stats_only`` is set to ``True``. Note that
    by default we raise a ``BulkIndexError`` when we encounter an error so
    options like ``stats_only`` only apply when ``raise_on_error`` is set to
    ``False``.

    When errors are being collected original document data is included in the
    error dictionary which can lead to an extra high memory usage. If you need
    to process a lot of data and want to ignore/collect errors please consider
    using the :func:`~elasticsearch.helpers.streaming_bulk` helper which will
    just return the errors and not store them in memory.


    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterator containing the actions
    :arg stats_only: if `True` only report number of successful/failed
        operations instead of just number of successful and a list of error responses

    Any additional keyword arguments will be passed to
    :func:`~elasticsearch.helpers.streaming_bulk` which is used to execute
    the operation, see :func:`~elasticsearch.helpers.streaming_bulk` for more
    accepted parameters.
    """
    success, failed = 0, 0

    # list of errors to be collected is not stats_only
    errors = []

    # make streaming_bulk yield successful results so we can count them
    kwargs['yield_ok'] = True
    for ok, item in streaming_bulk(client, actions, **kwargs):
        # go through request-reponse pairs and detect failures
        if not ok:
            if not stats_only:
                errors.append(item)
            failed += 1
        else:
            success += 1

    return success, failed if stats_only else errors
