import fire
import sys
import asyncio
from common.elasticsearch.doc_manager import mongo_docman
from module import sync

import logging

# todo: 统筹logging handler
logger = logging.getLogger('rts')

logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


class Sync:
    def __init__(self):
        print(logger.handlers)
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(mongo_docman.auto_committer.stop())

    def index_all(self):
        sync.delete_indies()
        sync.create_index()
        mongo_docman.log_block_chain.mark_ts()

        self.loop.run_until_complete(sync.index_all())
        self.loop.run_until_complete(mongo_docman.stop())
        self.loop.close()

    def index(self, *indices):
        if isinstance(indices, tuple):
            for idx in indices:
                if not hasattr(sync, idx):
                    logger.error('Invalid index "{0}"'.format(idx))
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
