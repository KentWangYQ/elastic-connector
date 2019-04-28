import config
from ..bson_serializer import BSONSerializer
from .mongo_doc_manager import DocManager

mongo_docman = DocManager(hosts=config.CONFIG.ELASTICSEARCH.get('hosts'),
                          client_options={
                              'timeout': 120,
                              'retry_on_timeout': True,
                              'sniff_on_start': False,
                              'sniff_on_connection_fail': False,
                              'sniffer_timeout': 60,
                              'max_retries': 3,
                              'serializer': BSONSerializer()
                          },
                          auto_commit=False,
                          # auto_commit_interval=10,
                          )
