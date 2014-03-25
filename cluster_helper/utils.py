import math

def convert_mb(kb, unit):
    UNITS = {"B": -2,
             "KB": -1,
             "MB": 0,
             "GB": 1,
             "TB": 2}
    assert unit in UNITS, ("%s not a valid unit, valid units are %s."
                           % (unit, UNITS.keys()))
    return int(float(kb) / float(math.pow(1024, UNITS[unit])))
