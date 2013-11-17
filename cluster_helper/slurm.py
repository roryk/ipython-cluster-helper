import subprocess

def get_accounts(user):
    """
    get all accounts a user can use to submit a job
    """
    cmd = "sshare -p --noheader"
    out, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    accounts = []
    for line in out.splitlines():
        line = line.split('|')
        account = line[0].strip()
        account_user = line[1].strip()
        if account and account_user == user:
            accounts.append(account)

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

def get_account_for_queue(queue):
    """
    selects all the accounts this user has access to that can submit to
    the queue and returns the first one
    """
    user = get_user()
    possible_accounts = get_accounts(user)
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

def get_max_timelimit_for_queue(queue):
    cmd = 'sinfo -o "%l" --noheader -p {0}'.format(queue)
    out, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    return out.strip()


def get_slurm_attributes(queue, resources):
    slurm_atrs = {}
    # specially handled resource specifications
    special_resources = set(["machines", "account", "timelimit"])
    default_tl = "7-00:00:00" # 1 week default
    if resources:
        for parm in resources.split(";"):
            k, v = [ a.strip() for a in  parm.split('=') ]
            slurm_atrs[k] = v
    if "account" not in slurm_atrs:
        slurm_atrs["account"] = get_account_for_queue(queue)
    if "timelimit" not in slurm_atrs:
        tl = get_max_timelimit_for_queue(queue)
        if tl == "infinite":
            tl = default_tl
        slurm_atrs["timelimit"] = tl
    # reconstitute any general attributes to pass along to slurm
    out_resources = []
    for k in slurm_atrs:
        if k not in special_resources:
            out_resources.append("%s=%s" % (k, slurm_atrs[k]))
    return ";".join(out_resources), slurm_atrs
