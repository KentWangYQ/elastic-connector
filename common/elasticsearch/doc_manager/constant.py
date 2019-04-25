from enum import Enum

DEFAULT_COMMIT_INTERVAL = 5
DEFAULT_SEND_INTERVAL = 5

DEFAULT_MAX_BULK = 1000


class ActionStatus(Enum):
    processing = 'processing'
    success = 'success'
    failed = 'failed'
