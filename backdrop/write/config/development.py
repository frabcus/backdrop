DATABASE_NAME = "backdrop"
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
LOG_LEVEL = "DEBUG"
SINGLE_SIGN_ON = True
SECRET_KEY = "something unique and secret"
BACKDROP_ADMIN_UI_HOST = "http://backdrop-admin.dev.gov.uk"
BUCKET_AUTO_ID = {
    "lpa_volumes": ("key", "start_at", "end_at")
}

try:
    from development_environment import *
except ImportError:
    from development_environment_sample import *
