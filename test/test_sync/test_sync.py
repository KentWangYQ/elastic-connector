import unittest
from module import sync


class SyncTest(unittest.TestCase):
    def test_create_index(self):
        sync.create_index()

    def test_delete_indies(self):
        sync.delete_indies()
