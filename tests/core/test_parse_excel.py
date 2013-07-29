import unittest
from hamcrest import assert_that, only_contains
from backdrop.core.errors import ParseError

from backdrop.core.parse_excel import parse_excel
from tests.support.test_helpers import fixture_path, d_tz


class ParseExcelTestCase(unittest.TestCase):
    def _parse_excel(self, file_name):
        file_stream = open(fixture_path(file_name))
        return parse_excel(file_stream)

    def test_parse_an_xlsx_file(self):
        assert_that(self._parse_excel("data.xlsx"),
                    only_contains(
                        {"name": "Pawel", "age": 27, "nationality": "Polish"},
                        {"name": "Max", "age": 35, "nationality": "Italian"}))

    def test_parse_xlsx_dates(self):
        assert_that(self._parse_excel("dates.xlsx"),
                    only_contains(
                        {"date": d_tz(2013, 12, 3, 13, 30)},
                        {"date": d_tz(2013, 12, 4)}))

    def test_parse_xls_file(self):
        assert_that(self._parse_excel("xlsfile.xls"),
                    only_contains(
                        {"date": d_tz(2013, 12, 3, 13, 30), "name": "test1", "number": 12},
                        {"date": d_tz(2013, 12, 4), "name": "test2", "number": 34}))

    def test_parse_xlsx_with_error(self):
        self.assertRaises(ParseError, self._parse_excel, "error.xlsx")
