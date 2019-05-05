from . import tracks


def create_index():
    tracks.create_index()


async def index_all():
    await tracks.index_all()


def real_time_sync():
    tracks.real_time_sync()
