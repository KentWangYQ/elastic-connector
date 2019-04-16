class Config:
    MONGO = {
        'uri': 'mongodb://username:pwd@192.168.1.119:27017/',
        'oplog_ns': 'test-db.*',
        'oplog_ns_include': []
    }
    ELASTICSEARCH = {
        'hosts': ['http://192.168.1.148:9210'],
        'timeout': 1000 * 60 * 10
    }
