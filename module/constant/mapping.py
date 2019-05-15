es_errors = {
    'namespace': {
        'error': 'errors.exception'
    },
    'settings': {
        'number_of_shards': 3,
        'number_of_replicas': 1
    },
    'mappings': {}
}

tracks = {
    'index': 'rts_test',
    'types': {
        'merchant': {
            'namespace': 'rts_test.merchant',
            'collection': 'merchants'
        },
        'impression_track': {
            'namespace': 'rts_test.impressiontrack',
            'collection': 'impressiontracks'
        },
        'act_share_detail': {
            'namespace': 'rts_test.actsharedetail',
            'collection': 'actsharedetails'
        }
    },
    'routing': 'tracks',
    'mappings': {
        'merchant': {},
        'impressiontrack': {
            '_parent': {
                'type': 'merchant'
            }
        },
        'actsharedetail': {
            '_parent': {
                'type': 'merchant'
            }
        }
    },
    'settings': {
        'number_of_shards': 3,
        'number_of_replicas': 1
    }
}
