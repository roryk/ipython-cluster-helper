import os
import subprocess

LSF_CONF_FILENAME = "lsf.conf"
LSF_CONF_ENV = ["LSF_CONFDIR", "LSF_ENVDIR"]
DEFAULT_LSF_UNITS = "KB"
DEFAULT_RESOURCE_UNITS = "MB"

def get_conf_file(env):
    conf_path = os.environ.get(env)
    conf_file = os.path.join(conf_path, LSF_CONF_FILENAME) if conf_path else None
    if conf_file and os.path.exists(conf_file):
        return conf_file
    else:
        return None

def get_lsf_units_from_conf():
    """
    if lsf.conf files are found, look for LSF_UNIT_FOR_LIMITS in them
    """
    for env in LSF_CONF_ENV:
        conf_file = get_conf_file(env)
        if conf_file:
            with open(conf_file) as conf_handle:
                lsf_units = get_lsf_units_from_stream(conf_handle)
            if lsf_units:
                return lsf_units
    return None

def get_lsf_units_from_stream(stream):
    for k, v in tokenize_conf_stream(stream):
        if k == "LSF_UNIT_FOR_LIMITS":
            return v
    return None

def tokenize_conf_stream(conf_handle):
    """
    convert the key=val pairs in a LSF config stream to tuples of tokens
    """
    for line in conf_handle:
        if line.startswith("#"):
            continue
        tokens = line.split("=")
        if len(tokens) != 2:
            continue
        yield (tokens[0].strip(), tokens[1].strip())

def get_lsf_units_from_lsadmin():
    cmd = ["lsadmin", "showconf", "lim"]
    try:
        output = subprocess.check_output(cmd)
    except:
        return None
    return get_lsf_units_from_stream(output.split("\n"))

def get_lsf_units(resource=False):
    """
    check if we can find LSF_UNITS_FOR_LIMITS in lsadmin and lsf.conf
    files, preferring the value from lsadmin
    """
    lsf_units = get_lsf_units_from_lsadmin()
    if lsf_units:
        return lsf_units

    lsf_units = get_lsf_units_from_conf()
    if lsf_units:
        return lsf_units

    # -R usage units are in MB, not KB by default
    if resource:
        return DEFAULT_RESOURCE_UNITS
    else:
        return DEFAULT_LSF_UNITS


if __name__ == "__main__":
    print get_lsf_units()
