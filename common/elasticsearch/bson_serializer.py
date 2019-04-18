import bson.json_util
from elasticsearch.serializer import JSONSerializer


class BSONSerializer(JSONSerializer):
    def loads(self, s):
        return bson.json_util.loads(s)

    def dumps(self, data):
        if isinstance(data, dict) and '_id' in data:
            del data['_id']
        return bson.json_util.dumps(data, json_options=bson.json_util.STRICT_JSON_OPTIONS)
