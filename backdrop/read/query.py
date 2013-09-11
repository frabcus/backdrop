from collections import namedtuple

import pytz
from backdrop.core.timeseries import parse_period
from backdrop.core.timeutils import parse_time_as_utc
from backdrop.read.response import *


def utc(dt):
    return dt.replace(tzinfo=pytz.UTC)


def if_present(func, value):
    """Apply the given function to the value and return if it exists"""
    if value is not None:
        return func(value)


def parse_request_args(request_args):
    args = dict()

    args['start_at'] = if_present(parse_time_as_utc,
                                  request_args.get('start_at'))

    args['end_at'] = if_present(parse_time_as_utc,
                                request_args.get('end_at'))

    def boolify(value):
        return {
            "true": True,
            "false": False,
        }.get(value, value)

    def parse_filter_by(filter_by):
        key, value = filter_by.split(':', 1)

        return [key, boolify(value)]

    args['filter_by'] = map(parse_filter_by, request_args.getlist('filter_by'))

    args['period'] = if_present(parse_period,
                                request_args.get('period'))

    args['group_by'] = request_args.get('group_by')

    args['sort_by'] = if_present(lambda sort_by: sort_by.split(':', 1),
                                 request_args.get('sort_by'))

    args['limit'] = if_present(int, request_args.get('limit'))

    args['collect'] = []
    for collect_arg in request_args.getlist('collect'):
        if ':' in collect_arg:
            args['collect'].append(tuple(collect_arg.split(':')))
        else:
            args['collect'].append((collect_arg, 'default'))

    return args


_Query = namedtuple(
    '_Query',
    'start_at end_at filter_by period group_by sort_by limit collect'
)


class Query(_Query):
    @classmethod
    def create(cls,
               start_at=None, end_at=None, filter_by=None, period=None,
               group_by=None, sort_by=None, limit=None, collect=None):
        return Query(start_at, end_at, filter_by or [], period,
                     group_by, sort_by, limit, collect or [])

    @classmethod
    def parse(cls, request_args):
        args = parse_request_args(request_args)
        return Query(**args)

    def to_mongo_query(self):
        mongo_query = {}
        if self.start_at or self.end_at:
            mongo_query["_timestamp"] = {}
            if self.end_at:
                mongo_query["_timestamp"]["$lt"] = self.end_at
            if self.start_at:
                mongo_query["_timestamp"]["$gte"] = self.start_at
        if self.filter_by:
            mongo_query.update(self.filter_by)
        return mongo_query

    @property
    def query_type(self):
        if self.group_by and self.period:
            return "period_grouped"
        elif self.group_by:
            return "grouped"
        elif self.period:
            return "period"
        else:
            return "standard"

    # @property
    # def response_class(self):
    #     # TODO: handle this
    #     return {
    #         "period_grouped": 
    #     }

    def execute(self, repository):
        method = {
            "period_grouped": self.__execute_period_group_query,
            "grouped": self.__execute_grouped_query,
            "period": self.__execute_period_query,
            "standard": self.__execute_query,
        }[self.query_type]

        return method(repository)

    def __get_period_key(self):
        return self.period.start_at_key

    def __execute_period_group_query(self, repository):
        period_key = self.__get_period_key()

        cursor = repository.multi_group(
            self.group_by, period_key, self,
            sort=self.sort_by, limit=self.limit,
            collect=self.collect
        )

        results = PeriodGroupedData(cursor, period=self.period)

        if self.start_at and self.end_at:
            results.fill_missing_periods(
                self.start_at, self.end_at, collect=self.collect)

        return results

    def __execute_grouped_query(self, repository):
        cursor = repository.group(self.group_by, self, self.sort_by,
                                  self.limit, self.collect)

        results = GroupedData(cursor)
        return results

    def __execute_period_query(self, repository):
        period_key = self.__get_period_key()
        sort = [period_key, "ascending"]
        cursor = repository.group(
            period_key, self,
            sort=sort, limit=self.limit, collect=self.collect
        )

        results = PeriodData(cursor, period=self.period)

        if self.start_at and self.end_at:
            results.fill_missing_periods(
                self.start_at, self.end_at, collect=self.collect)

        return results

    def __execute_query(self, repository):
        cursor = repository.find(
            self, sort=self.sort_by, limit=self.limit)

        results = SimpleData(cursor)
        return results
