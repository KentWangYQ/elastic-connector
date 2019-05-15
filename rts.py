import logging
import asyncio
from common.mongo import oplog_client
from module import sync

logger = logging.getLogger('rts')


def main():
    sync.real_time_sync()
    while True:
        @oplog_client.on('data')
        def on_data(data):
            logger.debug('[Oplog tail] a_ts:%s op:%s c_ns:%s' % (data.get('ts'), data.get('op'), data.get('ns')))

        @oplog_client.on('error')
        def on_error(error, data):
            logger.warning('Oplog tail error%r', error)

        loop = asyncio.get_event_loop()
        logger.info('Start real time sync')
        loop.run_until_complete(oplog_client.tail())
        # loop.run_forever()
        loop.close()


if __name__ == '__main__':
    main()
