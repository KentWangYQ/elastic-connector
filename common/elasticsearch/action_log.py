from enum import Enum
from common import util


class SVActionLogBlockStatus(Enum):
    processing = 'processing'
    done = 'done'


class ActionLogBlock:
    def __init__(self,
                 prev_block_hash,
                 actions,
                 time=util.utc_now_timestamp_ms(), ):
        self._id = hash(None)  # todo: 使用sha256作为id
        self.prev_block_hash = prev_block_hash
        self.time = time
        self.merkle_root = self._get_merkle_root()
        self.actions = actions
        self.actions_count = len(self.actions) if actions else 0

    def _get_merkle_root(self):
        # todo 实现获取merkle root
        return ''


class SVActionLogBlock(ActionLogBlock):
    """Simple verify log block

    Only has block header, no actions detail.
    """

    def __init__(self,
                 prev_block_hash,
                 actions,
                 time=util.utc_now_timestamp_ms(),
                 status=SVActionLogBlockStatus.processing):
        super().__init__(prev_block_hash=prev_block_hash, time=time, actions=actions)
        self.first_action = actions[0]
        self.last_action = actions[-1]
        self.actions_count = len(actions)
        self.status = status


GENESIS_BLOCK = ActionLogBlock(prev_block_hash=None, actions=[])  # todo: 确定创世区块的参数
