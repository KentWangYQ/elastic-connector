import re
import copy
import bson.json_util
import logging
import asyncio
import threading
import elasticsearch_async
from . import constant
from .doc_manager_base import DocManagerBase


class DocManager(DocManagerBase):
    def __init__(self, hosts, *args, **kwargs):
        client_options = kwargs.get('client_options')
        self._es = elasticsearch_async.AsyncElasticsearch(hosts=hosts, **client_options)

    def __del__(self):
        self._es.transport.close()

    def create(self, index, doc_type, id, doc, *args, **kwargs):
        raise NotImplementedError()

    def index(self, index, doc_type, doc, id=None, *args, **kwargs):
        if '_id' in doc:
            del doc['_id']
        return self._es.index(index=index, doc_type=doc_type, body=doc, id=id, *args, **kwargs)

    def update(self, index, doc_type, id, doc=None, *args, **kwargs):
        return self._es.update(index=index, doc_type=doc_type, id=id, body={'doc': doc}, *args, **kwargs)

    def delete(self, index, doc_type, id, *args, **kwargs):
        return self._es.delete(index=index, doc_type=doc_type, id=id, *args, **kwargs)

    def bulk(self, docs, index=None, doc_type=None, action=None, *args, **kwargs):
        body = []
        for doc in docs:
            if '_id' in doc:
                del doc['_id']
            # get action
            action = action
            if 'action' in doc:
                action = doc.pop('action')
            assert action in ['create', 'index', 'update', 'delete'], 'Invalid action in bulk docs!'
            # get metadata
            metadata = {}
            valid_metadata = ['_index', '_type', '_id', '_parent', '_routing']
            if 'metadata' in doc:
                metadata = doc.pop('metadata')
                assert isinstance(metadata, dict), 'metadata must be a dict!'
                for m in metadata:
                    if m not in valid_metadata:
                        logging.warning('"%s" is not a valid metadata' % m)
                        del metadata[m]
            body.append({action: metadata})

            if action in ['create', 'index']:
                body.append(doc)
            elif action == 'update':
                body.append({'doc': doc})
            elif action == 'delete':
                pass
        return self._es.bulk(body=body, index=index, doc_type=doc_type, *args, **kwargs)

    def update_by_query(self, index, doc, query, doc_type=None, *args, **kwargs):
        inline = ''
        if doc:
            for k in doc:
                ks = copy.deepcopy(k)
                for key in re.findall(r'\.\d+', k):
                    ks = ks.replace(key, '[%s]' % key[1:])
                inline += 'ctx._source.%s=params.%s' % (ks, k.replace('.', ''))

            for k in doc:
                if '.' in k:
                    kr = k.replace('.', '')
                    doc[kr] = doc[k]
                    del doc[k]
        else:
            inline += 'ctx._source={};'

        query = query or {'match_all': {}}
        body = {'query': query, 'script': {'inline': inline, 'params': doc, 'lang': 'painless'}}
        return self._es.update_by_query(index=index, doc_type=doc_type, body=body, *args, **kwargs)

    def delete_by_query(self, index, query, doc_type=None, *args, **kwargs):
        query = query or {'match_all': {}}
        body = {'query': query}
        return self._es.delete_by_query(index=index, body=body, doc_type=doc_type, *args, **kwargs)


class DocManager1(object):  # todo: 反向处理base
    def __init__(self,
                 url,
                 auto_commit_interval=constant.DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id',
                 chunk_size=constant.DEFAULT_MAX_BULK,
                 meta_index_name='mongodb_meta',
                 meta_type='mongodb_meta',
                 attachment_field='content',
                 **kwargs):
        client_options = kwargs.get('client_options')
        if type(url) is not list:
            url = [url]
        self.elastic = elasticsearch_async.AsyncElasticsearch(hosts=url, **client_options)
        self._formatter = None  # todo 实现formatter
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

    def _index_and_mapping(self, namespace):
        index, doc_type = namespace.lower().split('.', 1)
        return index, doc_type

    def apply_update(self, doc, update_spec):
        assert isinstance(doc, dict), 'doc must be a dict'
        assert isinstance(update_spec, dict), 'update_spec must be a dict'
        doc.update(update_spec)
        return doc

    def upsert(self, doc, namespace, timestamp, is_update=False):
        index, doc_type = self._index_and_mapping(namespace)
        doc_id = str(doc.pop('_id'))

        _original_op_type = 'update' if is_update else 'insert'
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
        doc['_id'] = doc_id

    def delete(self):
        pass

    def bulk_upsert(self):
        pass

    def index(self, action, meta_action):
        self.bulk_buffer.add_upsert(action, meta_action)

        if self.bulk_buffer.count() >= self.chunk_size or self.auto_commit_interval == 0:
            self.commit()

    def commit(self):
        pass

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

        self.action_buffer[action.get('_id')] = action
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
