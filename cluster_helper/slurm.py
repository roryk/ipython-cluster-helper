import subprocess

DEFAULT_TIME = "1-00:00:00"

class SlurmTime(object):
    """
    parse a SlurmTime of the format hours-days, hours-days:minutes,
    hours-days:minutes:seconds, minutes, hours:minutes, 
    hours:minutes:seconds. assume 'infinite' means 365 days
    """
    def __init__(self, time):
        self.days = 0
        self.hours = 0
        self.minutes = 0
        self.seconds = 0
        self._parse_time(time)

    def _parse_time(self, time):
        if time == "infinite": 
            self.days = 365
        else:
            day_tokens = time.split("-")
            if len(day_tokens) == 2:
                self.days = int(day_tokens[0])
                time = day_tokens[1]
            time_tokens = time.split(":")
            if len(time_tokens) == 3:
                self.hours, self.minutes, self.seconds = map(int, time_tokens)
            elif len(time_tokens) == 2:
                self.hours, self.minutes = map(int, time_tokens)
            elif self.days:
                self.hours = int(time_tokens[0])
            else:
                self.minutes = int(time_tokens[0])

    def __cmp__(self, slurmtime):
        cdays = slurmtime.days
        chours = slurmtime.hours
        cminutes = slurmtime.minutes
        cseconds = slurmtime.seconds
        if self.days < cdays:
            return -1
        if self.days > cdays:
            return 1
        if self.hours < chours:
            return -1
        if self.hours > cdays:
            return 1
        if self.minutes < cminutes:
            return -1
        if self.minutes > cminutes:
            return 1
        if self.seconds < cseconds:
            return -1
        if self.seconds > cseconds:
            return 1
        return 0

    def __repr__(self):
        return("%02d-%02d:%02d:%02d" % (self.days, self.hours, self.minutes, self.seconds))

def get_accounts(user):
    """
    get all accounts a user can use to submit a job
    """
    cmd = "sshare -p --noheader"
    out, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    accounts = []
    has_accounts = False
    for line in out.splitlines():
        line = line.split('|')
        account = line[0].strip()
        account_user = line[1].strip()
        if account and account_user == user:
            has_accounts = True
            accounts.append(account)

    if not has_accounts:
        return None
    accounts = set(accounts)
    try:
        assert not set(['']).issuperset(accounts)
    except AssertionError:
        raise ValueError("No usable accounts were found for user \'{0}\'".format(user))

    return accounts

def get_user():
    out, err = subprocess.Popen("whoami", shell=True,
                                stdout=subprocess.PIPE).communicate()
    return out.strip()

def accounts_with_access(queue):
    cmd = "sinfo --noheader -p {0} -o %g".format(queue)
    out, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    return set([x.strip() for x in out.split(",")])

def get_max_timelimit(queue):
    cmd = "sinfo --noheader -p {0}".format(queue)
    out, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    max_limit = out.split()[2].strip()
    time_limit = None if max_limit == "inifinite" else max_limit
    return max_limit

def get_account_for_queue(queue):
    """
    selects all the accounts this user has access to that can submit to
    the queue and returns the first one
    """
    user = get_user()
    possible_accounts = get_accounts(user)
    if not possible_accounts:
        return None
    queue_accounts = accounts_with_access(queue)
    try:
        assert not set(['']).issuperset(queue_accounts)
    except AssertionError:
        raise ValueError("No accounts accessible by user \'{0}\' have access to queue \'{1}\'. Accounts found were: {2}".format(
                         user, queue, ", ".join(possible_accounts)))
    accts = list(possible_accounts.intersection(queue_accounts))
    if "all" not in queue_accounts and len(accts) > 0:
        return accts[0]
    else:
        return possible_accounts.pop()

def set_timelimit(slurm_atrs):
    """
    timelimit can be specified as timelimit, t or time. remap 
    t and time to timelimit, preferring timelimit
    """
    if "timelimit" in slurm_atrs:
        return slurm_atrs
    if "time" in slurm_atrs:
        slurm_atrs["timelimit"] = slurm_atrs["time"]
        del slurm_atrs["time"]
        return slurm_atrs
    if "t" in slurm_atrs:
        slurm_atrs["timelimit"] = slurm_atrs["t"]
        del slurm_atrs["t"]
        return slurm_atrs
    slurm_atrs["timelimit"] = DEFAULT_TIME
    return slurm_atrs

def get_slurm_attributes(queue, resources):
    slurm_atrs = {}
    # specially handled resource specifications
    special_resources = set(["machines", "account", "timelimit"])
    if resources:
        for parm in resources.split(";"):
            k, v = [ a.strip() for a in  parm.split('=') ]
            slurm_atrs[k] = v
    if "account" not in slurm_atrs:
        account = get_account_for_queue(queue)
        if account:
            slurm_atrs["account"] = account
    slurm_atrs = set_timelimit(slurm_atrs)
    max_limit = get_max_timelimit(queue)
    if max_limit:
        slurm_atrs["timelimit"] = min(SlurmTime(slurm_atrs["timelimit"]), SlurmTime(max_limit))

    # reconstitute any general attributes to pass along to slurm
    out_resources = []
    for k in slurm_atrs:
        if k not in special_resources:
            out_resources.append("%s=%s" % (k, slurm_atrs[k]))
    return ";".join(out_resources), slurm_atrs
