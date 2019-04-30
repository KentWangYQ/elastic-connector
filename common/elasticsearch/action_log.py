import hashlib
import json
from merkletools import MerkleTools
from common import util

HASH = hashlib.md5  # default hash algorithm


class JSONSerializer(object):
    mimetype = 'application/json'

    @staticmethod
    def loads(s):
        return json.loads(s)

    @staticmethod
    def dumps(data):
        if isinstance(data, str):
            return data

        return json.dumps(data)


class ActionLogBlockStatus:
    processing = 'processing'
    done = 'done'


class ActionLogBlock:
    def __init__(self,
                 prev_block_hash,
                 actions,
                 create_time=util.utc_now(),
                 status=ActionLogBlockStatus.processing,
                 serializer=JSONSerializer()):
        self._serializer = serializer
        self.prev_block_hash = prev_block_hash
        # unix timestamp, 13 bytes
        self.create_time = create_time
        self.status = status
        self.actions = actions
        self.actions_count = len(self.actions)
        # Merkle tree for actions
        self.merkle_tree = self._make_merkle_tree()

        self.merkle_root_hash = self._get_merkle_root()  # depend on self.actions
        self.id = self.__hash__()  # depend self.on prev_block_hash, self.create_time and self.merkle_root_hash

    def _make_merkle_tree(self):
        mt = MerkleTools(hash_type=HASH().name)
        r = list(map(self._serializer.dumps, self.actions))
        mt.add_leaf(r, True)
        mt.make_tree()
        return mt

    def _get_merkle_root(self):
        return self.merkle_tree.get_merkle_root()

    def __hash__(self):
        """Hash for string: prev_block_hash + create_time + merkle_root_hash"""
        return HASH(
            ''.join(map(str, [self.prev_block_hash, self.create_time, self.merkle_root_hash])).encode()).hexdigest()

    def to_dict(self):
        return {
            'id': self.id,
            'prev_block_hash': self.prev_block_hash,
            'create_time': self.create_time,
            'actions': self.actions,
            'actions_count': self.actions_count,
            'merkle_root_hash': self.merkle_root_hash,
            'status': self.status
        }


class SVActionLogBlock(ActionLogBlock):
    """Simple verify block

    Only has block header, no merkle tree and actions detail.
    """

    def __init__(self,
                 prev_block_hash,
                 actions,
                 create_time=util.utc_now(),
                 status=ActionLogBlockStatus.processing,
                 serializer=JSONSerializer()):
        super().__init__(prev_block_hash=prev_block_hash,
                         create_time=create_time,
                         status=status,
                         actions=actions,
                         serializer=serializer)
        if self.actions_count > 0:
            self.first_action = actions[0]
            self.last_action = actions[-1]

        # Simple verify block do NOT has merkle_tree and actions
        self.merkle_tree = None
        self.actions = []
        self.status = status

    def to_dict(self):
        d = super().to_dict()
        d.update({
            'first_action': self.first_action,
            'last_action': self.last_action
        })
        return d


# GENESIS_BLOCK
GENESIS_BLOCK = ActionLogBlock(prev_block_hash=None, actions=[], create_time=1556219384204)
