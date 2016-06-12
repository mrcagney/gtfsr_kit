
import os
import datetime as dt
import json
from functools import wraps


PROJECT_ROOT = os.path.abspath(os.path.join(
  os.path.dirname(__file__), '../'))
SECRETS_PATH = os.path.join(PROJECT_ROOT, 'secrets.json')
TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

def get_secret(secret, secrets_path=SECRETS_PATH):
    """
    Get the given setting variable or return explicit exception.
    """
    with open(secrets_path) as src:
        d = json.loads(src.read())
    try:
        return d[secret]
    except KeyError:
        raise ValueError("Set the {0} secrets variable".format(secret))

def format_timestamp(t, format_str=TIMESTAMP_FORMAT):
    """
    Given a POSIX timestamp (float), format it as a string
    in the given format
    """
    t = dt.datetime.fromtimestamp(t)
    return dt.datetime.strftime(t, format_str)

def time_it(f):
    """
    Decorate function ``f`` to measure and print elapsed time when executed.
    """
    @wraps(f)
    def wrap(*args, **kwargs):
        t1 = dt.datetime.now()
        print('Timing {!s}...'.format(f.__name__))
        print(t1, '  Began process')
        result = f(*args, **kwargs)
        t2 = dt.datetime.now()
        minutes = (t2 - t1).seconds/60
        print(t2, '  Finished in %.2f min' % minutes)    
        return result
    return wrap
