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
            'namespace': 'tracks.merchant',
            'collection': 'merchants'
        },
        # 'impression_track': {
        #     'namespace': 'tracks.impressiontrack',
        #     'collection': 'impressiontracks'
        # },
        'impression_track': {  # todo: for test remove
            'namespace': 'rts_test.rt',
            'collection': 'impressiontracks'
        },
        'act_share_detail': {
            'namespace': 'tracks.actsharedetail',
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
