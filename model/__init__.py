from collections import deque
import pydash as _
from aiostream import stream

import config
import motor.motor_asyncio

mongo_client = motor.motor_asyncio.AsyncIOMotorClient(config.CONFIG.MONGO.get('uri'))
db = mongo_client.get_database()


class PopulatePath:
    def __init__(self, pop):
        self.local_field = pop.get('local_field')
        self.let = pop.get('let')

        self.from_ = pop.get('from')
        self.foreign_collection = db[self.from_]
        self.foreign_field = pop.get('foreign_field')
        self.filter_ = pop.get('filter') or {}
        self.projection = pop.get('projection')
        self.as_ = pop.get('as')

        self.pop_fields = pop.get('pop_field')


class BaseModel(motor.motor_asyncio.AsyncIOMotorCollection):
    def populates(self, pop_fields, filter_=None, projection=None, sort=None, skip=0, limit=0, batch_size=1000):
        pop_paths = self._gen_pop_pathes(pop_fields)

        cursor = self.find(filter=filter_, projection=projection, sort=sort, skip=skip, limit=limit,
                           batch_size=batch_size)

        return self._f(pop_paths, cursor, batch_size)

    def _gen_pop_pathes(self, pop_fields):
        pop_list, pop_paths = deque(), []
        pop_list.extend(pop_fields.values())
        while len(pop_list):
            pop = pop_list.popleft()
            if pop:
                pop_paths.append(PopulatePath(pop))
                if 'pop_fields' in pop:
                    pop_list.extend(pop.get('pop_fields').values())
        return pop_paths

    async def _f(self, pop_paths, cursor, batch_size):
        async with stream.chunks(cursor, batch_size).stream() as chunks:
            async for chunk in chunks:
                l_docs = chunk
                for pop_path in pop_paths:
                    l_docs = await self._populate(l_docs, pop_path)
                for doc in l_docs:
                    yield doc

    async def _populate(self, l_docs: list, pop_path: PopulatePath):
        if pop_path.let:
            # 字段预处理
            def _t(doc, k, expr):
                v = expr(doc)
                if v:
                    doc = _.set_(doc, k, v)
                return doc

            l_docs = [_t(doc, k, expr) for k, expr in pop_path.let.items() for doc in l_docs]

        if pop_path.from_ not in await db.list_collection_names():
            raise Exception(f'Collection {pop_path.from_} does NOT exists')

        l_keys = set([_.get(doc, pop_path.local_field) for doc in l_docs if _.has(doc, pop_path.local_field)])

        if not l_keys or len(l_keys) <= 0:
            return l_docs

        f_docs = {doc.get('_id'): doc async for doc in pop_path.foreign_collection.find(
            filter={**pop_path.filter_, **{pop_path.foreign_field: {'$in': list(l_keys)}}},
            projection=pop_path.projection)}

        del l_keys  # 释放资源

        for doc in l_docs:
            _k = _.get(doc, pop_path.local_field)
            if _k in f_docs:
                doc.update(_.set_({}, pop_path.as_, f_docs.get(_k)))

        return l_docs


_model_mapping = {
    'merchant': 'merchants',
    'impression_track': 'impressiontracks',
    'act_share_detail': 'actsharedetails',
}

for k, v in _model_mapping.items():
    obj = db[v]
    obj.__class__ = BaseModel
    locals()[k] = obj

__all__ = _model_mapping.keys()
