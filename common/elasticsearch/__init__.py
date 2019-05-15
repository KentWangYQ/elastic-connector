import config
import elasticsearch_async

from .bson_serializer import BSONSerializer

es_client = elasticsearch_async.AsyncElasticsearch(hosts=config.CONFIG.ELASTICSEARCH.get('hosts'),
                                                   timeout=30,
                                                   retry_on_timeout=True,
                                                   sniff_on_start=True,
                                                   sniff_on_connection_fail=True,
                                                   raise_on_sniff_error=False,
                                                   sniff_timeout=1,
                                                   max_retries=3,
                                                   serializer=BSONSerializer()
                                                   )
