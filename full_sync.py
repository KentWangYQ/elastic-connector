import fire
from common.elasticsearch.doc_manager import mongo_docman


class Sync:
    def index_all(self):
        pass

    def index(self, *indices):
        if isinstance(indices, tuple):
            for index in indices:
                if not hasattr(elasticsearch, index):
                    logger.warning('elasticsearch has no index "{0}"'.format(index))
                    return
            for index in indices:
                getattr(elasticsearch, index).index()
        else:
            logger.error('Invalidate type, indices must be str or tuple.')


fire.Fire(Sync)
