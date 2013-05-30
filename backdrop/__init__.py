import os
import statsd as _statsd

__title__ = 'backdrop'
__version__ = '0.1.0'
__author__ = 'GDS Team'

__all__ = ['statsd']


class StatsClient(object):
    """Wrap statsd.StatsClient to allow bucket to be added to stat"""
    def __init__(self, statsd):
        self._statsd = statsd

    def __getattr__(self, item):
        if item in ['timer', 'timing', 'incr', 'decr', 'gauge']:
            def func(stat, *args, **kwargs):
                bucket = kwargs.pop('bucket', 'unknown')
                stat = '%s.%s' % (bucket, stat)

                return getattr(self._statsd, item)(stat, *args, **kwargs)
            return func
        else:
            return getattr(self._statsd, item)

statsd = StatsClient(
    _statsd.StatsClient(prefix=os.getenv("GOVUK_STATSD_PREFIX")))
