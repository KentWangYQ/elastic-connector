import unittest
import full_sync


class FullSyncTest(unittest.TestCase):
    def test_index_all(self):
        f_sync = full_sync.Sync()
        f_sync.index_all()

    def test_index(self):
        f_sync = full_sync.Sync()
        f_sync.index('tracks')
