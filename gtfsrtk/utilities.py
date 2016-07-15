import os
import datetime as dt
import json
from functools import wraps
from pathlib import Path 


PROJECT_DIR = Path(os.path.abspath(os.path.join(
  os.path.dirname(__file__), '../'))).resolve()
SECRETS_PATH = PROJECT_DIR/'secrets.json'
TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

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

def get_secret(key, path=SECRETS_PATH):
    """
    Get the given setting variable or return explicit exception.
    """
    with path.open() as src:
        d = json.loads(src.read())
    return d[key]

def timestamp_to_str(t, format=TIMESTAMP_FORMAT, inverse=False):
    """
    Given a POSIX timestamp (float) ``t``, format it as a string
    in the given format.
    If ``inverse``, then do the inverse, that is, assume ``t`` is 
    a string in the given format and return its corresponding timestamp.
    If ``format is None``, then cast ``t`` as a float (if not ``inverse``)
    or string (if ``inverse``) directly.
    """
    if not inverse:
        if format is None:
            result = str(t)
        else:
            result = dt.datetime.fromtimestamp(t).strftime(format)
    else:
        if format is None:
            result = float(t)
        else:
            result = dt.datetime.strptime(t, format).timestamp()
    return result
