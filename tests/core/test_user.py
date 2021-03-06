from hamcrest import assert_that, is_
from nose.tools import *
from backdrop.core.user import UserConfig


class TestUserConfig(object):
    def test_buckets_defaults_to_empty_list(self):
        user_config = UserConfig("test@example.com")

        assert_that(user_config.buckets, is_([]))

    def test_creation_fails_if_buckets_is_not_a_list(self):
        assert_raises(ValueError, UserConfig, "test@example.com", "invalid")

    def test_creation_fails_if_buckets_is_not_a_list_of_strings(self):
        assert_raises(ValueError, UserConfig, "test@example.com", ["foo", 2, 3])
