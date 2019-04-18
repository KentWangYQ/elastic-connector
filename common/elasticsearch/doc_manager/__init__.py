import config
from ..bson_serializer import BSONSerializer
from .mongo_doc_manager import DocManager

mongo_doc_manager = DocManager(hosts=config.CONFIG.ELASTICSEARCH.get('hosts'),
                               client_options={
                                   'timeout': 120,
                                   'retry_on_timeout': True,
                                   'sniff_on_start': True,
                                   'sniff_on_connection_fail': True,
                                   'sniffer_timeout': 1200,
                                   'max_retries': 3,
                                   'serializer': BSONSerializer()})
