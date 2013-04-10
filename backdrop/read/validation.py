from dateutil import parser
import api
from ..core.validation import value_is_valid_datetime_string, valid, invalid
import re

MONGO_FIELD_REGEX = re.compile(r'^[A-Za-z-_]+$')
MESSAGES = {
    "disallowed": {
        "no_grouping": "querying for raw data has been disallowed",
        "non-midnight": "start_at and end_at must be at midnight",
        "non-week-length": "difference between start_at and end_at must be 7 "
                           "days"
    },
    'unrecognised': 'An unrecognised parameter was provided',
    'start_at': {
        'invalid': 'start_at is not a valid datetime'
    },
    'end_at': {
        'invalid': 'end_at is not a valid datetime'
    },
    'filter_by': {
        'colon': 'filter_by must be a field name and value separated by '
                 'a colon (:) eg. authority:Westminster',
        'dollar': 'filter_by must not start with a $'
    },
    'period': {
        'invalid': 'Unrecognised grouping for period. Supported periods '
                   'include: week',
        'group': 'Cannot group on two equal keys',
        'sort': 'Cannot use sort_by for period queries - period queries '
                'are always sorted by time'
    },
    'group_by': {
        'internal': 'Cannot group by internal fields, internal fields start '
                    'with an underscore'
    },
    'sort_by': {
        'colon': 'sort_by must be a field name and sort direction separated '
                 'by a colon (:) eg. authority:ascending',
        'direction': 'Unrecognised sort direction. Supported directions '
                     'include: ascending, descending'
    },
    'limit': {
        'invalid': 'limit must be a positive integer'
    },
    'collect': {
        'no_grouping': 'collect is only allowed when grouping',
        'invalid': 'collect must be a valid field name',
        'internal': 'Cannot collect internal fields, internal fields start '
                    'with an underscore',
        'groupby_field': "Cannot collect by a field that is used for group_by"
    }
}


def is_a_raw_query(request_args):
    if 'group_by' in request_args:
        return False
    if 'period' in request_args:
        return False
    return True


def request_length_valid(start_at, end_at):
    start_at = parser.parse(start_at)
    end_at = parser.parse(end_at)
    delta = end_at - start_at
    return delta.days >= 7


def dates_on_midnight(start_at, end_at):
    start_at = parser.parse(start_at)
    end_at = parser.parse(end_at)
    return (start_at.minute + start_at.second + start_at.hour) == 0 \
        and (end_at.minute + end_at.second + end_at.hour) == 0


class Validator(object):
    def __init__(self, request_args, **context):
        self.errors = []
        self.validate(request_args, context)

    def invalid(self):
        return len(self.errors) > 0

    def add_error(self, message):
        self.errors.append(invalid(message))

    def validate(self, request_args, context):
        raise NotImplementedError


class ParameterValidator(Validator):
    def __init__(self, request_args):
        self.allowed_parameters = set([
            'start_at',
            'end_at',
            'filter_by',
            'period',
            'group_by',
            'sort_by',
            'limit',
            'collect'
        ])
        super(ParameterValidator, self).__init__(request_args)

    def _unrecognised_parameters(self, request_args):
        return set(request_args.keys()) - self.allowed_parameters

    def validate(self, request_args, context):
        if len(self._unrecognised_parameters(request_args)) > 0:
            self.add_error("An unrecognised parameter was provided")


class DatetimeValidator(Validator):
    def validate(self, request_args, context):
        if context['param_name'] in request_args:
            if not value_is_valid_datetime_string(
                    request_args[context['param_name']]):
                self.add_error('%s is not a valid datetime'
                               % context['param_name'])


class PositiveIntegerValidator(Validator):
    def validate(self, request_args, context):
        if context['param_name'] in request_args:
            try:
                if int(request_args[context['param_name']]) < 0:
                    raise ValueError()
            except ValueError:
                self.add_error("%s must be a positive integer"
                               % context['param_name'])


class FilterByValidator(Validator):
    def validate(self, request_args, context):
        filter_by = request_args.get('filter_by', None)
        if filter_by:
            if filter_by.find(':') < 0:
                self.add_error(
                    'filter_by must be a field name and value separated by '
                    'a colon (:) eg. authority:Westminster')
            if filter_by.startswith('$'):
                self.add_error(
                    'filter_by must not start with a $')


class ParameterMustBeThisValidator(Validator):
    def validate(self, request_args, context):
        if context['param_name'] in request_args:
            if request_args[context['param_name']] != context['must_be_this']:
                self.add_error('Unrecognised grouping for period. '
                               'Supported periods include: week')


class SortByValidator(Validator):
    def _unrecognised_direction(self, sort_by):
        return not re.match(r'^.+:(ascending|descending)$', sort_by)

    def validate(self, request_args, context):
        if 'sort_by' in request_args:
            if 'period' in request_args and 'group_by' not in request_args:
                self.add_error("Cannot sort for period queries without "
                               "group_by. Period queries are always sorted "
                               "by time.")
            if request_args['sort_by'].find(':') < 0:
                self.add_error(
                    'sort_by must be a field name and sort direction separated'
                    ' by a colon (:) eg. authority:ascending')
            if self._unrecognised_direction(request_args['sort_by']):
                self.add_error('Unrecognised sort direction. Supported '
                               'directions include: ascending, descending')


class GroupByValidator(Validator):
    def validate(self, request_args, context):
        if 'group_by' in request_args:
            if request_args['group_by'].startswith('_'):
                self.add_error('Cannot group by internal fields, '
                               'internal fields start with an underscore')


class CollectValidator(Validator):
    def validate(self, request_args, context):
        if 'collect' in request_args:
            if 'group_by' not in request_args:
                self.add_error('collect is only allowed when grouping')
            if not MONGO_FIELD_REGEX.match(request_args['collect']):
                self.add_error('collect must be a valid field name')
            if request_args['collect'].startswith('_'):
                self.add_error('Cannot collect internal fields, '
                               'internal fields start '
                               'with an underscore')
            if 'group_by' in request_args:
                if request_args['collect'] == request_args['group_by']:
                    self.add_error("Cannot collect by a field that is "
                                   "used for group_by")


class RawQueryValidator(Validator):
    def validate(self, request_args, context):
        if is_a_raw_query(request_args):
            self.add_error("querying for raw data is not allowed")


class MinimumTimeSpanValidator(Validator):
    def validate(self, request_args, context):
        if self._is_valid_date_query(request_args):
            start_at = parser.parse(request_args['start_at'])
            end_at = parser.parse(request_args['end_at'])
            delta = end_at - start_at
            if delta.days < context['length']:
                self.add_error('The minimum time span for a query is 7 days')

    def _is_valid_date_query(self, request_args):
        if 'start_at' not in request_args:
            return False
        if 'end_at' not in request_args:
            return False
        if not value_is_valid_datetime_string(request_args['start_at']):
            return False
        if not value_is_valid_datetime_string(request_args['end_at']):
            return False
        return True



def validate_request_args(request_args):
    request_args_copy = request_args.copy()

    start_at = request_args_copy.pop('start_at', None)
    end_at = request_args_copy.pop('end_at', None)

    validators = [
        ParameterValidator(request_args),
        DatetimeValidator(request_args, param_name='start_at'),
        DatetimeValidator(request_args, param_name='end_at'),
        FilterByValidator(request_args),
        ParameterMustBeThisValidator(request_args, param_name='period',
                                     must_be_this='week'),
        SortByValidator(request_args),
        GroupByValidator(request_args),
        PositiveIntegerValidator(request_args, param_name='limit'),
        CollectValidator(request_args),
    ]

    if api.app.config['PREVENT_RAW_QUERIES']:
        validators.append(RawQueryValidator(request_args))
        validators.append(MinimumTimeSpanValidator(request_args, length=7))



    for validator in validators:
        if validator.invalid():
            return validator.errors[0]

    if api.app.config['PREVENT_RAW_QUERIES']:
        # if is_a_raw_query(request_args):
        #     return invalid(MESSAGES['disallowed']['no_grouping'])
        if start_at and end_at:
            # if not request_length_valid(start_at, end_at):
            #     return invalid(MESSAGES['disallowed']['non-week-length'])
            if not dates_on_midnight(start_at, end_at):
                return invalid(MESSAGES['disallowed']['non-midnight'])

    return valid()


# def validate(request_args):
#     errors = [validator.validate(request_args) for
#       validator in get_validators(request_args)]
#
#     return len(errors) > 0, errors
