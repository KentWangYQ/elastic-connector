import re
import copy
import elasticsearch_async
from .doc_manager_base import DocManagerBase


class DocManager(DocManagerBase):
    def __init__(self, hosts, *args, **kwargs):
        client_options = kwargs.get('client_options')
        self._es = elasticsearch_async.AsyncElasticsearch(hosts=hosts, **client_options)

    def create(self, index, doc_type, id, doc, params=None, *args, **kwargs):
        raise NotImplementedError()

    def index(self, index, doc_type, doc, id=None, params={}, *args, **kwargs):
        if '_id' in doc:
            del doc['_id']
        return self._es.index(index=index, doc_type=doc_type, body=doc, id=id, params=params)

    def update(self, index, doc_type, id, doc=None, params=None, *args, **kwargs):
        return self.update_by_query(index, doc_type, {'term': {'_id': id}}, doc, params, *args, **kwargs)

    def delete(self, index, doc_type, id, params=None, *args, **kwargs):
        return self._es.delete(index, doc_type, id, params, *args, *kwargs)

    def bulk(self, docs, index=None, doc_type=None, params=None, *args, **kwargs):
        raise NotImplementedError()

    def update_by_query(self, index, doc, query, doc_type=None, params=None, *args, **kwargs):
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
        return self._es.update_by_query(index, doc_type, body, params, *args, **kwargs)

    def delete_by_query(self, index, query, doc_type=None, params=None, *args, **kwargs):
        query = query or {'match_all': {}}
        body = {'query': query}
        return self._es.delete_by_query(index, body, doc_type, params, *args, **kwargs)
