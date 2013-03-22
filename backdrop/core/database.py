from itertools import groupby
from bson import Code
from pprint import pprint
from pymongo import MongoClient


def build_query(**params):
    def ensure_has_timestamp(q):
        if '_timestamp' not in q:
            q['_timestamp'] = {}
        return q

    query = {}
    if 'start_at' in params:
        query = ensure_has_timestamp(query)
        query['_timestamp']['$gte'] = params['start_at']
    if 'end_at' in params:
        query = ensure_has_timestamp(query)
        query['_timestamp']['$lt'] = params['end_at']

    if 'filter_by' in params:
        for key, value in params['filter_by']:
            query[key] = value
    return query


class Database(object):
    def __init__(self, host, port, name):
        self._mongo = MongoClient(host, port)
        self.name = name

    def alive(self):
        return self._mongo.alive()

    def get_repository(self, bucket_name):
        return Repository(self._mongo[self.name][bucket_name])

    @property
    def connection(self):
        return self._mongo[self.name]


class Repository(object):
    def __init__(self, collection):
        self._collection = collection

    @property
    def name(self):
        return self._collection.name

    def find(self, query):
        return self._collection.find(query).sort('_timestamp', -1)

    def group(self, group_by, query):
        return self._group([group_by], query)

    def save(self, obj):
        self._collection.save(obj)

    def multi_group(self, key1, key2, query):
        if key1 == key2:
            raise GroupingError("Cannot group on two equal keys")
        results = self._group([key1, key2], query)

        output = nested_merge([key1, key2], results)

        result = []
        for key1_value, value in sorted(output.items()):
            result.append({
                key1: key1_value,
                "_count": sum(doc['_count'] for doc in value.values()),
                "_group_count": len(value),
                key2: value
            })

        return result

    def _group(self, keys, query):
        results = self._collection.group(
            key=keys,
            condition=query,
            initial={'_count': 0},
            reduce=Code("""
                function(current, previous) { previous._count++; }
                """)
        )
        for result in results:
            for key in keys:
                if result[key] is None:
                    return []
        return results


class GroupingError(ValueError):
    pass


def nested_merge(keys, results):
    output = {}
    for result in results:
        output = _inner_merge(output, keys, result)
    return output


def _inner_merge(output, keys, value):
    if len(keys) == 0:
        return value
    key = value.pop(keys[0])
    if key not in output:
        output[key] = {}
    output[key].update(_inner_merge(output[key], keys[1:], value))

    return output