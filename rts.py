import sys
import logging
import asyncio
from common.mongo import oplog_client
from module import sync

# todo: 统筹logging handler
logger = logging.getLogger('rts')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)

# create console handler and set level to debug
che = logging.StreamHandler()
che.setLevel(logging.WARNING)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
che.setFormatter(formatter)

# add ch to logger
logger.addHandler(che)
logger.addHandler(ch)


def main():
    sync.real_time_sync()
    while True:
        @oplog_client.on('data')
        def on_data(data):
            logger.debug('[Oplog tail] a_ts:%s op:%s c_ns:%s' % (data.get('ts'), data.get('op'), data.get('ns')))

        @oplog_client.on('error')
        def on_error(error, data):
            logger.warning('Oplog tail error', error)

        loop = asyncio.get_event_loop()
        logger.info('Start real time sync')
        loop.run_until_complete(oplog_client.tail())
        # loop.run_forever()
        loop.close()


if __name__ == '__main__':
    main()
