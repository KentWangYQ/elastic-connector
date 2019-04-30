import asyncio
import config
from common.elasticsearch.doc_manager import mongo_docman
from common.mongo import oplog_client
from module import sync


def main():
    sync.rt()
    while True:
        @oplog_client.on('data')
        def on_data(data):
            print({'a_ts': data.get('ts'), 'b_op': data.get('op'), 'c_ns': data.get('ns')})

        @oplog_client.on('error')
        def on_error(error, data):
            print(error)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(oplog_client.tail())
        loop.close()


if __name__ == '__main__':
    main()
