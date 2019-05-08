from . import tracks


def create_index():
    tracks.create_index()


async def index_all():
    await tracks.index_all()


def real_time_sync():
    tracks.real_time_sync()


sync_manager_list = set()


def register_sync_manager(manager):
    sync_manager_list.add(manager)
