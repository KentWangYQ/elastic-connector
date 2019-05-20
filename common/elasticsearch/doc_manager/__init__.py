from config import CONFIG
from . import constant
from ..bson_serializer import BSONSerializer
from .mongo_doc_manager import DocManager

mongo_docman = DocManager(hosts=CONFIG.ELASTICSEARCH.get('hosts'),
                          client_options={
                              'timeout': 30,
                              'retry_on_timeout': True,
                              'sniff_on_start': False,
                              'sniff_on_connection_fail': False,
                              'raise_on_sniff_error': False,
                              'sniff_timeout': 10,
                              'max_retries': 3,
                              'serializer': BSONSerializer()
                          },
                          log_index='%s%s%s' % (
                              CONFIG.ELASTICSEARCH.get('index_prefix', ''),
                              constant.MONGO_LOG_BLOCK_INDEX,
                              CONFIG.ELASTICSEARCH.get('index_suffix', '')),
                          log_type='%s%s%s' % (
                              CONFIG.ELASTICSEARCH.get('index_prefix', ''),
                              constant.MONGO_LOG_BLOCK_TYPE,
                              CONFIG.ELASTICSEARCH.get('index_suffix', '')),
                          error_index='%s%s%s' % (
                              CONFIG.ELASTICSEARCH.get('index_prefix', ''),
                              constant.MONGO_ERROR_INDEX,
                              CONFIG.ELASTICSEARCH.get('index_suffix', '')),
                          error_type='%s%s%s' % (
                              CONFIG.ELASTICSEARCH.get('index_prefix', ''),
                              constant.MONGO_ERROR_TYPE,
                              CONFIG.ELASTICSEARCH.get('index_suffix', '')),
                          auto_commit=True
                          )
