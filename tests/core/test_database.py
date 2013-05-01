import unittest
from hamcrest import assert_that, is_
from mock import Mock, patch
from backdrop.core import database
from backdrop.core.database import Repository, InvalidSortError
from backdrop.read.query import Query
from tests.support.test_helpers import d_tz


class NestedMergeTestCase(unittest.TestCase):
    def setUp(self):
        self.dictionaries = [
            {'a': 1, 'b': 2, 'c': 3},
            {'a': 1, 'b': 1, 'c': 3},
            {'a': 2, 'b': 1, 'c': 3}
        ]

    def test_nested_merge_merges_dictionaries(self):
        output = database.nested_merge(['a', 'b'], [], self.dictionaries)

        assert_that(output[0], is_({
            "a": 1,
            "_count": 0,
            "_group_count": 2,
            "_subgroup": [
                {"b": 1, "c": 3},
                {"b": 2, "c": 3},
            ],
        }))
        assert_that(output[1], is_({
            "a": 2,
            "_count": 0,
            "_group_count": 1,
            "_subgroup": [
                {"b": 1, "c": 3}
            ],
        }))

    def test_nested_merge_squashes_duplicates(self):
        output = database.nested_merge(['a'], [], self.dictionaries)
        assert_that(output, is_([
            {'a': 1, 'b': 2, 'c': 3},
            {'a': 2, 'b': 1, 'c': 3}
        ]))


class TestRepository(unittest.TestCase):
    def setUp(self):
        self.mongo = Mock()
        self.repo = Repository(self.mongo)

    @patch('backdrop.core.time.now')
    def test_save_document_adding_timestamps(self, now):
        now.return_value = d_tz(2013, 4, 9, 13, 32, 5)

        self.repo.save({"name": "Gummo"})

        self.mongo.save.assert_called_once_with({
            "name": "Gummo",
            "_updated_at": d_tz(2013, 4, 9, 13, 32, 5)
        })

    # =========================
    # Tests for repository.find
    # =========================
    def test_find(self):
        self.mongo.find.return_value = "a_cursor"

        results = self.repo.find(
            Query.create(filter_by=[["plays", "guitar"]]),
            sort= ["name", "ascending"])

        self.mongo.find.assert_called_once_with({"plays": "guitar"},
                                                ["name", "ascending"], None)
        assert_that(results, is_("a_cursor"))

    def test_find_with_descending_sort(self):
        self.mongo.find.return_value = "a_cursor"

        results = self.repo.find(
            Query.create(filter_by=[["plays", "guitar"]]),
            sort= ["name", "descending"])

        self.mongo.find.assert_called_once_with({"plays": "guitar"},
                                                ["name", "descending"], None)
        assert_that(results, is_("a_cursor"))

    def test_find_with_default_sorting(self):
        self.mongo.find.return_value = "a_cursor"

        results = self.repo.find(
            Query.create(filter_by=[["plays", "guitar"]]))

        self.mongo.find.assert_called_once_with({"plays": "guitar"},
                                                ["_timestamp", "ascending"],
                                                None)
        assert_that(results, is_("a_cursor"))

    def test_find_with_limit(self):
        self.mongo.find.return_value = "a_cursor"

        results = self.repo.find(
            Query.create(filter_by=[["plays", "guitar"]]), limit=10)

        self.mongo.find.assert_called_once_with({"plays": "guitar"},
                                                ["_timestamp", "ascending"],
                                                10)
        assert_that(results, is_("a_cursor"))

    def test_sort_raises_error_if_sort_does_not_have_two_elements(self):
        self.assertRaises(
            InvalidSortError,
            self.repo.find,
            Query.create(), ["a_key"]
        )

    def test_sort_raises_error_if_sort_direction_invalid(self):
        self.assertRaises(
            InvalidSortError,
            self.repo.find,
            Query.create(), ["a_key", "blah"]
        )
