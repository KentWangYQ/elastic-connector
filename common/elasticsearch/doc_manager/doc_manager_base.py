from abc import ABCMeta, abstractmethod
from elasticsearch.client.utils import query_params


class DocManagerBase(metaclass=ABCMeta):
    @abstractmethod
    @query_params('parent', 'pipeline', 'refresh', 'routing', 'timeout',
                  'timestamp', 'ttl', 'version', 'version_type', 'wait_for_active_shards')
    def create(self, index, doc_type, id, doc, params=None, *args, **kwargs):
        """"""

    @abstractmethod
    @query_params('op_type', 'parent', 'pipeline', 'refresh', 'routing',
                  'timeout', 'timestamp', 'ttl', 'version', 'version_type',
                  'wait_for_active_shards')
    def index(self, index, doc_type, doc, id=None, params=None, *args, **kwargs):
        """"""

    @abstractmethod
    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'lang', 'parent', 'refresh', 'retry_on_conflict', 'routing', 'timeout',
                  'timestamp', 'ttl', 'version', 'version_type', 'wait_for_active_shards')
    def update(self, index, doc_type, id, doc=None, params=None, *args, **kwargs):
        """"""

    @abstractmethod
    @query_params('parent', 'refresh', 'routing', 'timeout', 'version',
                  'version_type', 'wait_for_active_shards')
    def delete(self, index, doc_type, id, params=None, *args, **kwargs):
        """"""

    @abstractmethod
    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'pipeline', 'refresh', 'routing', 'timeout', 'wait_for_active_shards')
    def bulk(self, docs, index=None, doc_type=None, params=None, action=None, *args, **kwargs):
        """"""

    @abstractmethod
    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'conflicts',
                  'default_operator', 'df', 'expand_wildcards', 'from_',
                  'ignore_unavailable', 'lenient', 'pipeline', 'preference', 'q',
                  'refresh', 'request_cache', 'requests_per_second', 'routing', 'scroll',
                  'scroll_size', 'search_timeout', 'search_type', 'size', 'slices',
                  'sort', 'stats', 'terminate_after', 'timeout', 'version',
                  'version_type', 'wait_for_active_shards', 'wait_for_completion')
    def update_by_query(self, index, doc, query, doc_type=None, params=None, *args, **kwargs):
        """"""

    @abstractmethod
    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'conflicts',
                  'default_operator', 'df', 'expand_wildcards', 'from_',
                  'ignore_unavailable', 'lenient', 'preference', 'q', 'refresh',
                  'request_cache', 'requests_per_second', 'routing', 'scroll',
                  'scroll_size', 'search_timeout', 'search_type', 'size', 'slices',
                  'sort', 'stats', 'terminate_after', 'timeout', 'version',
                  'wait_for_active_shards', 'wait_for_completion')
    def delete_by_query(self, index, query, doc_type=None, params=None, *args, **kwargs):
        """"""
