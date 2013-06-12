from os import getenv

from flask import Flask, request, jsonify, g, abort
from backdrop import statsd
from backdrop.core.log_handler \
    import create_request_logger, create_response_logger
from backdrop.write.permissions import Permissions
from backdrop.write.admin_ui import use_single_sign_on
from backdrop.write import admin_ui, parse_and_store

from ..core.errors import ParseError, ValidationError
from ..core.validation import bucket_is_valid
from ..core import database, log_handler, records, cache_control
from ..core.bucket import Bucket

from .validation import bearer_token_is_valid


def setup_logging():
    log_handler.set_up_logging(app, "write",
                               getenv("GOVUK_ENV", "development"))


def environment():
    return getenv("GOVUK_ENV", "development")


app = Flask(__name__, static_url_path="/_user/static")

# Configuration
app.config.from_object(
    "backdrop.write.config.%s" % environment()
)

app.config['USER_SCOPE'] = "/_user"

db = database.Database(
    app.config['MONGO_HOST'],
    app.config['MONGO_PORT'],
    app.config['DATABASE_NAME']
)

setup_logging()

app.before_request(create_request_logger(app))
app.after_request(create_response_logger(app))

app.permissions = Permissions(app.config["PERMISSIONS"])

if use_single_sign_on(app):
    app.secret_key = app.config['SECRET_KEY']
    admin_ui.setup(app, db)


@app.errorhandler(500)
@app.errorhandler(405)
@app.errorhandler(404)
def exception_handler(e):
    app.logger.exception(e)

    bucket_name = getattr(g, 'bucket_name', request.path)
    statsd.incr("write.error", bucket=bucket_name)

    code = getattr(e, 'code', 500)
    name = getattr(e, 'name', "Internal Error")

    return jsonify(status='error', message=name), code


@app.route("/", methods=['GET'])
def index():
    return "write.backdrop root path."


@app.route('/_status', methods=['GET'])
@cache_control.nocache
def health_check():
    if db.alive():
        return jsonify(status='ok', message='database seems fine')
    else:
        return jsonify(status='error',
                       message='cannot connect to database'), 500


@app.route('/<bucket_name>', methods=['POST'])
@cache_control.nocache
def post_to_bucket(bucket_name):
    g.bucket_name = bucket_name

    if not bucket_is_valid(bucket_name):
        return abort(404)

    tokens = app.config['TOKENS']
    auth_header = request.headers.get('Authorization', None)

    if not bearer_token_is_valid(tokens, auth_header, bucket_name):
        statsd.incr("write_api.bad_token", bucket=g.bucket_name)
        return jsonify(status='error', message='Forbidden'), 403

    try:
        parse_and_store(
            db,
            load_json(request.json),
            bucket_name,
            app.logger)

        return jsonify(status='ok')
    except (ParseError, ValidationError) as e:
        return jsonify(status="error", message=str(e)), 400


def load_json(data):
    if data is None:
        raise ValidationError("Request must be JSON")

    if isinstance(data, list):
        return data
    else:
        return [data]


def start(port):
    # this method only gets run on dev
    # app.debug = True
    app.run(host='0.0.0.0', port=port)
    app.logger.info("Backdrop Write API started")
