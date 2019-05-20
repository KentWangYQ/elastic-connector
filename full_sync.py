import fire
import asyncio
from common.elasticsearch.doc_manager import mongo_docman
from module import sync

import logging

logger = logging.getLogger('rts')


class Sync:
    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(mongo_docman.auto_committer.stop())

    def index_all(self):
        sync.delete_indies()
        sync.create_index()

        self.loop.run_until_complete(sync.index_all())
        self.loop.run_until_complete(mongo_docman.stop())
        self.loop.close()

    def index(self, *indices):
        if isinstance(indices, tuple):
            for idx in indices:
                if not hasattr(sync, idx):
                    logger.error('Invalid index "%s"' % idx)
                    return
            for idx in indices:
                getattr(sync, idx).delete_index()
                getattr(sync, idx).create_index()

            future = asyncio.wait([getattr(sync, idx).index_all() for idx in indices])

            self.loop.run_until_complete(future)
            self.loop.run_until_complete(mongo_docman.stop())
            self.loop.close()

        else:
            logger.error('Invalidate type, indices must be str or tuple.')


fire.Fire(Sync)
