import logging
from . import tracks

# __all__ = ['car_products', 'es_errors', 'car_change_plans', 'activities', 'tracks', 'intents']
__all__ = ['tracks']

logger = logging.getLogger('rts')


def create_index():
    tracks.create_index()
    logger.info('All index created')


async def index_all():
    logger.info('Start index all indies...')
    await tracks.index_all()


def delete_indies():
    tracks.delete_indies()
    logger.info('All indies deleted')


def delete_all():
    tracks.delete_all()
    logger.info('All documents deleted')


def real_time_sync():
    tracks.real_time_sync()
    logger.info('Register real time sync')
