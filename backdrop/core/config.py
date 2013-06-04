"""Load configuration for Flask apps
"""
import os


GOVUK_ENV = os.getenv('GOVUK_ENV', 'development')

ROOT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..'))

CONFIG_PATH = os.getenv('BACKDROP_CONFIG_ROOT',
                        os.path.join(ROOT_PATH, 'config'))


def load(app, name):
    """Load configuration for a given flask app"""
    load_config_file(app, '{0}.py'.format(GOVUK_ENV))
    load_config_file(app, '{0}/{1}.py'.format(name, GOVUK_ENV))

    load_legacy_config_file(app, name, GOVUK_ENV)


def load_config_file(app, path):
    """Load a config file from the config directory"""
    config_path = os.path.join(CONFIG_PATH, path)

    if os.path.exists(config_path):
        app.config.from_pyfile(config_path)


def load_legacy_config_file(app, name, env):
    """Load a config file from the legacy location if it exists"""
    path = os.path.join(ROOT_PATH, 'backdrop', name, '{0}.py'.format(env))
    if os.path.exists(path):
        app.config.from_object('backdrop.{0}.{1}'.format(name, env))
