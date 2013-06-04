import sys
import os

os.environ['GOVUK_ENV'] = 'test'

sys.path.append(
    os.path.join(os.path.dirname(__file__), '..')
)
