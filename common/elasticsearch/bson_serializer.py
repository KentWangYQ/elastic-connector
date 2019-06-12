import bson.json_util
from elasticsearch import serializer

_old_default = bson.json_util.default


def _default(obj, json_options=bson.json_util.DEFAULT_JSON_OPTIONS):
    if isinstance(obj, bson.ObjectId):
        return str(obj)

    res = _old_default(obj=obj, json_options=json_options)

    # override datetime format，from {$date:{datetime}} to datetime, remove $date
    if isinstance(obj, bson.json_util.datetime.datetime):
        if res['$date']:
            return res['$date']

    return res


bson.json_util.default = _default


class BSONSerializer(serializer.JSONSerializer):
    def loads(self, s):
        return bson.json_util.loads(s)

    def dumps(self, data):
        if isinstance(data, serializer.string_types):
            return data
        return bson.json_util.dumps(data, json_options=bson.json_util.RELAXED_JSON_OPTIONS)
