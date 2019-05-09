from . import tracks

# __all__ = ['car_products', 'es_errors', 'car_change_plans', 'activities', 'tracks', 'intents']
__all__ = ['tracks']


def create_index():
    tracks.create_index()


async def index_all():
    await tracks.index_all()


def delete_all():
    tracks.delete_all()


def delete_index():
    tracks.delete_index()


def real_time_sync():
    tracks.real_time_sync()
