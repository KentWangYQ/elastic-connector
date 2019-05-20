from config import CONFIG

_prefix = CONFIG.ELASTICSEARCH.get('index_prefix')
_suffix = CONFIG.ELASTICSEARCH.get('index_suffix')


def _gen_index_by_env(index):
    return '%s%s%s' % (_prefix, index, _suffix)


tracks = {
    'index': _gen_index_by_env('tracks'),
    'types': {
        'merchant': {
            'type': 'merchant',
            'collection': 'merchants'
        },
        'impression_track': {
            'type': 'impressiontrack',
            'collection': 'impressiontracks'
        },
        'act_share_detail': {
            'type': 'actsharedetail',
            'collection': 'actsharedetails'
        }
    },
    'routing': '1',
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
        'number_of_shards': 1,
        'number_of_replicas': 1
    }
}
