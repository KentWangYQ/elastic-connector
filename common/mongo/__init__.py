import config
from .oplog import Oplog
from common.elasticsearch.doc_manager import mongo_docman

oplog_client = oplog.Oplog(uri=config.CONFIG.MONGO.get('oplog_uri'),
                           ts=mongo_docman.log_block_chain.last_ts,
                           ns=config.CONFIG.MONGO.get('oplog_ns'),
                           exclude_ns=config.CONFIG.MONGO.get('oplog_ns_exclude'),
                           include_ns=config.CONFIG.MONGO.get('oplog_ns_include'),
                           include_ops=['i', 'u', 'd'],
                           )
