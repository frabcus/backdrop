import csv
from .errors import ParseError


def parse_csv(incoming_data):
    return list(
        parse_rows(
            unicode_csv_dict_reader(incoming_data, 'utf-8')
        )
    )


def is_empty_row(row):
    return all(not v for v in row.values())


def parse_rows(data):
    for datum in data:
        if None in datum.keys():
            raise ParseError(
                'Some rows ins the CSV file contain more values than columns')
        if None in datum.values():
            raise ParseError(
                'Some rows in the CSV file contain fewer values than columns')
        if not is_empty_row(datum):
            yield datum


class CommentedFile:
    def __init__(self, reader, commentstring="#"):
        self._reader = reader
        self._commentstring = commentstring

    def next(self):
        line = self._reader.next()
        while line.startswith(self._commentstring):
            line = self._reader.next()
        return line

    def __iter__(self):
        return self


class UnicodeCsvReader(object):
    def __init__(self, reader, encoding):
        self._reader = reader
        self._encoding = encoding

    def next(self):
        try:
            return [self._decode(cell) for cell in self._reader.next()]
        except UnicodeError:
            raise ParseError("Non-UTF8 characters found.")

    @property
    def line_num(self):
        return self._reader.line_num

    def _decode(self, cell):
        return unicode(cell, self._encoding)


def unicode_csv_dict_reader(incoming_data, encoding):
    r = csv.DictReader(CommentedFile(incoming_data))
    r.reader = UnicodeCsvReader(r.reader, encoding)
    return r
