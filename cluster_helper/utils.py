import os
import math
import time

def convert_mb(kb, unit):
    UNITS = {"B": -2,
             "KB": -1,
             "MB": 0,
             "GB": 1,
             "TB": 2}
    assert unit in UNITS, ("%s not a valid unit, valid units are %s."
                           % (unit, UNITS.keys()))
    return int(float(kb) / float(math.pow(1024, UNITS[unit])))

def safe_makedir(dname):
    """Make a directory if it doesn't exist, handling concurrent race conditions.
    """
    if not dname:
        return dname
    num_tries = 0
    max_tries = 5
    while not os.path.exists(dname):
        # we could get an error here if multiple processes are creating
        # the directory at the same time. Grr, concurrency.
        try:
            os.makedirs(dname)
        except OSError:
            if num_tries > max_tries:
                raise
            num_tries += 1
            time.sleep(2)
    return dname
