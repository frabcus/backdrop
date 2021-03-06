from unittest import TestCase
from hamcrest import *
from backdrop.read.query import Query
from tests.support.test_helpers import d_tz


class TestBuild_query(TestCase):
    def test_build_query_with_start_at(self):
        query = Query.create(start_at=d_tz(2013, 3, 18, 18, 10, 05))
        assert_that(query.to_mongo_query(), is_(
            {"_timestamp": {"$gte": d_tz(2013, 03, 18, 18, 10, 05)}}))

    def test_build_query_with_end_at(self):
        query = Query.create(end_at = d_tz(2012, 3, 17, 17, 10, 6))
        assert_that(query.to_mongo_query(), is_(
            {"_timestamp": {"$lt": d_tz(2012, 3, 17, 17, 10, 6)}}))

    def test_build_query_with_start_and_end_at(self):
        query = Query.create(
            start_at = d_tz(2012, 3, 17, 17, 10, 6),
            end_at = d_tz(2012, 3, 19, 17, 10, 6))
        assert_that(query.to_mongo_query(), is_({
            "_timestamp": {
                "$gte": d_tz(2012, 3, 17, 17, 10, 6),
                "$lt": d_tz(2012, 3, 19, 17, 10, 6)
            }
        }))

    def test_build_query_with_filter(self):
        query = Query.create(filter_by= [[ "foo", "bar" ]])
        assert_that(query.to_mongo_query(), is_({ "foo": "bar" }))

    def test_build_query_with_multiple_filters(self):
        query = Query.create(filter_by= [[ "foo", "bar" ], ["foobar", "yes"]])
        assert_that(query.to_mongo_query(),
                    is_({ "foo": "bar", "foobar": "yes" }))
