from .bucket_new import Bucket


class BucketRepository(object):
    def __init__(self, collection):
        self._collection = collection

    def save(self, bucket):
        doc = {
            "_id": bucket.name,
            "name": bucket.name,
            "raw_queries_allowed": bucket.raw_queries_allowed,
        }
        self._collection.save(doc)

    def retrieve(self, name):
        doc = self._collection.find_one({"name": name})
        if doc is None:
            return None
        del doc["_id"]
        return Bucket(**doc)


class RecordRepository(object):
    def __init__(self, db):
        self._db = db

    def query(self, bucket, query):
        collection = self._db.get_collection(bucket.name)

        method = self.__get_query_method(query.query_type)

        results = method(collection, query)

        # return query.response_type(results)
        return results

    def __get_query_method(self, query_type):
        return {
            "period_grouped": self.__execute_period_group_query,
            "grouped": self.__execute_grouped_query,
            "period": self.__execute_period_query,
            "standard": self.__execute_standard_query,
        }[query_type]

    def __to_mongo_query(self, query):
        mongo_query = {}
        if query.start_at or query.end_at:
            mongo_query["_timestamp"] = {}
            if query.end_at:
                mongo_query["_timestamp"]["$lt"] = query.end_at
            if query.start_at:
                mongo_query["_timestamp"]["$gte"] = query.start_at
        if query.filter_by:
            mongo_query.update(query.filter_by)
        return mongo_query

    def __execute_period_group_query(self, collection, query):
        pass

    def __execute_grouped_query(self, collection, query):
        pass

    def __execute_period_query(self, collection, query):
        pass

    def __execute_standard_query(self, collection, query):
        results = collection.find(self.__to_mongo_query(query))
        return results


class PeriodGroupQueryHandler(object):
    def __init__(self, collection, query):
        pass

    def query(self):
        self.collection.group(...)
        # performs query and returns raw results


    def prepare_results(self):
        # prepare mongo specific results for generic response class


    def response_class(self):
        # return response class

class GroupedQueryHandler(object):
    def __init__(self, collection, query):
        self.collection.group(...)

class PeriodQueryHandler(object):
    def __init__(self, collection, query):
        pass

class StandardQueryHandler(object):
    def __init__(self, collection, query):
        self.collection.find(...)
