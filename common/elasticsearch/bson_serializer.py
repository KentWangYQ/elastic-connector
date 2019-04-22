import bson.json_util
from elasticsearch import serializer


class BSONSerializer(serializer.JSONSerializer):
    def loads(self, s):
        return bson.json_util.loads(s)

    def dumps(self, data):
        if isinstance(data, serializer.string_types):
            return data
        return bson.json_util.dumps(data, json_options=bson.json_util.STRICT_JSON_OPTIONS)
