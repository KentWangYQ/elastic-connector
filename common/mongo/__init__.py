import config
from .oplog import Oplog

oplog_client = Oplog(uri=config.CONFIG.MONGO.get('oplog_uri'),
                     # ts=bson.Timestamp(time, inc),
                     ns=config.CONFIG.MONGO.get('oplog_ns'),
                     exclude_ns=config.CONFIG.MONGO.get('oplog_ns_exclude'),
                     include_ns=config.CONFIG.MONGO.get('oplog_ns_include'),
                     include_ops=['i', 'u', 'd'],
                     # limit=100,
                     )
