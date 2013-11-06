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
    return set(accounts)

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
    return list(possible_accounts.intersection(queue_accounts))[0]

def get_max_timelimit_for_queue(queue):
    cmd = 'sinfo -o "%l" --noheader -p {0}'.format(queue)
    out, err = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
    return out.strip()


def get_slurm_attributes(queue, resources):
    slurm_atrs = {}
    if resources:
        for parm in resources.split(";"):
            atr = parm.split('=')
            slurm_atrs[atr[0]] = atr[1]
    if "account" not in slurm_atrs:
        slurm_atrs["account"] = get_account_for_queue(queue)
    if "timelimit" not in slurm_atrs:
        slurm_atrs["timelimit"] = get_max_timelimit_for_queue(queue)

    return slurm_atrs
