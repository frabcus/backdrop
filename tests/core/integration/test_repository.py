from backdrop.core.repository import BucketRepository, RecordRepository
from backdrop.core.bucket_new import Bucket
from backdrop.read.query import Query
from hamcrest import assert_that, equal_to, is_
from mock import Mock


class TestBucketRepository(object):
    def test_saving_a_bucket(self):
        mongo_collection = Mock()
        bucket_repo = BucketRepository(mongo_collection)

        bucket = Bucket("bucket_name")

        bucket_repo.save(bucket)
        mongo_collection.save.assert_called_with({
            "_id": "bucket_name",
            "name": "bucket_name",
            "raw_queries_allowed": False,
        })

    def test_saving_a_bucket_with_raw_queries_allowed(self):
        mongo_collection = Mock()
        bucket_repo = BucketRepository(mongo_collection)

        bucket = Bucket("bucket_name", raw_queries_allowed=True)

        bucket_repo.save(bucket)
        mongo_collection.save.assert_called_with({
            "_id": "bucket_name",
            "name": "bucket_name",
            "raw_queries_allowed": True
        })

    def test_retrieving_a_bucket(self):
        mongo_collection = Mock()
        bucket_repo = BucketRepository(mongo_collection)

        mongo_collection.find_one.return_value = {
            "_id": "bucket_name",
            "name": "bucket_name",
            "raw_queries_allowed": False,
        }
        bucket = bucket_repo.retrieve(name="bucket_name")

        assert_that(bucket, equal_to(Bucket("bucket_name")))

    def test_retrieving_non_existent_bucket_returns_none(self):
        mongo_collection = Mock()
        bucket_repo = BucketRepository(mongo_collection)

        mongo_collection.find_one.return_value = None
        bucket = bucket_repo.retrieve(name="bucket_name")

        assert_that(bucket, is_(None))

    def test_retrieving_a_bucket_with_raw_queries_allowed(self):
        mongo_collection = Mock()
        bucket_repo = BucketRepository(mongo_collection)

        mongo_collection.find_one.return_value = {
            "_id": "bucket_name",
            "name": "bucket_name",
            "raw_queries_allowed": True,
        }
        bucket = bucket_repo.retrieve(name="bucket_name")

        assert_that(bucket, equal_to(Bucket("bucket_name", True)))


class TestRecordRepository(object):
    def setUp(self):
        self.db = Mock()
        self.collection = Mock()

        self.db.get_collection.return_value = self.collection
        self.record_repository = RecordRepository(self.db)

    def test_query(self):
        self.collection.find.return_value = [{
            "name": "foo"
        }]

        bucket = Bucket(name="bucket_name", raw_queries_allowed=True)
        query = Query.create()

        results = self.record_repository.query(bucket, query)

        self.collection.find.assert_called_with({})

        assert_that(results, is_([{"name":"foo"}]))

    def test_query_with_filter_by(self):
        self.collection.find.return_value = [{
            "name": "foo"
        }]

        bucket = Bucket(name="bucket_name", raw_queries_allowed=True)
        query = Query.create(filter_by=[("foo", "bar")])

        results = self.record_repository.query(bucket, query)

        self.collection.find.assert_called_with({"foo":"bar"})

        assert_that(results, is_([{"name":"foo"}]))

    def test_query_filter_by_and_sort_by(self):
        self.collection.find.return_value = [
            { "name": "foo" },
            { "name": "bar" }
        ]

        bucket = Bucket(name="bucket_name", raw_queries_allowed=True)
        query = Query.create(filter_by=[("foo", "bar")])
        #sort=["name", "ascending"]
        results = self.record_repository.query(bucket, query)

        self.collection.find.assert_called_with({"foo":"bar"}, ["name", "ascending"])

        assert_that(results, is_([{"name":"bar"},{"name":"foo"}]))
