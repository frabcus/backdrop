"""
Add _hour_start_at and _day_start_at field to all documents in all collections
"""
import logging

from backdrop.core.records import Record
from backdrop.core.timeutils import utc

log = logging.getLogger(__name__)


def up(db):
    for name in db.collection_names():
        log.info("Migrating collection: {0}".format(name))
        collection = db[name]
        query = {
            "_timestamp": {"$exists": True},
            "_day_start_at": {"$exists": False}
        }
        for document in collection.find(query):
            document['_timestamp'] = utc(document['_timestamp'])
            for attr in ['_updated_at', '_week_start_at', '_month_start_at']:
                if attr in document:
                    document.pop(attr)
            record = Record(document)

            collection.save(record.to_mongo())