import asyncio
from common.mongo import oplog_client
from module import sync


def main():
    sync.create_index()  # todo: rts是否需要create

    loop = asyncio.get_event_loop()
    loop.run_until_complete(sync.index_all())

    sync.real_time_sync()
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
