"""Distributed execution using an IPython cluster.

Uses IPython parallel to setup a cluster and manage execution:

http://ipython.org/ipython-doc/stable/parallel/index.html
Borrowed from Brad Chapman's implementation:
https://github.com/chapmanb/bcbio-nextgen/blob/master/bcbio/distributed/ipython.py
"""
import contextlib
import os
import pipes
import uuid
import shutil
import subprocess
import time

from IPython.parallel import Client
from IPython.parallel.apps import launcher
from IPython.utils import traitlets
from IPython.utils.traitlets import (List, Unicode, CRegExp)

# ## Custom launchers

timeout_params = ["--timeout=30", "--IPEngineApp.wait_for_url_file=960"]
controller_params = ["--nodb", "--hwm=1", "--scheme=lru"]

# ## Platform LSF
class BcbioLSFEngineSetLauncher(launcher.LSFEngineSetLauncher):
    """Custom launcher handling heterogeneous clusters on LSF.
    """
    cores = traitlets.Integer(1, config=True)
    default_template = traitlets.Unicode("""#!/bin/sh
#BSUB -q {queue}
#BSUB -J bcbio-ipengine[1-{n}]
#BSUB -oo bcbio-ipengine.bsub.%%J
#BSUB -n {cores}
#BSUB -R "span[hosts=1]"
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
    """ % (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
           ' '.join(timeout_params)))

    def start(self, n):
        self.context["cores"] = self.cores
        return super(BcbioLSFEngineSetLauncher, self).start(n)

class BcbioLSFControllerLauncher(launcher.LSFControllerLauncher):
    default_template = traitlets.Unicode("""#!/bin/sh
#BSUB -J bcbio-ipcontroller
#BSUB -oo bcbio-ipcontroller.bsub.%%J
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
    """%(' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
         ' '.join(controller_params)))
    def start(self):
        return super(BcbioLSFControllerLauncher, self).start()

# ## Sun Grid Engine (SGE)
class BcbioSGEEngineSetLauncher(launcher.SGEEngineSetLauncher):
    """Custom launcher handling heterogeneous clusters on SGE.
    """
    cores = traitlets.Integer(1, config=True)
    pename = traitlets.Unicode("", config=True)
    default_template = traitlets.Unicode("""#$ -V
#$ -cwd
#$ -b y
#$ -j y
#$ -S /bin/sh
#$ -q {queue}
#$ -N bcbio-ipengine
#$ -t 1-{n}
#$ -pe {pename} {cores}
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
"""% (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
      ' '.join(timeout_params)))

    def start(self, n):
        self.context["cores"] = self.cores
        self.context["pename"] = str(self.pename)
        return super(BcbioSGEEngineSetLauncher, self).start(n)

class BcbioSGEControllerLauncher(launcher.SGEControllerLauncher):
    default_template = traitlets.Unicode(u"""#$ -V
#$ -S /bin/sh
#$ -N ipcontroller
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
"""%(' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
     ' '.join(controller_params)))
    def start(self):
        return super(BcbioSGEControllerLauncher, self).start()

def _find_parallel_environment():
    """Find an SGE/OGE parallel environment for running multicore jobs.
    """
    for name in subprocess.check_output(["qconf", "-spl"]).strip().split():
        if name:
            for line in subprocess.check_output(["qconf", "-sp", name]).split("\n"):
                if line.startswith("allocation_rule") and line.find("$pe_slots") >= 0:
                    return name
    raise ValueError("Could not find an SGE environment configured for parallel execution. " \
                     "See %s for SGE setup instructions." %
                     "https://blogs.oracle.com/templedf/entry/configuring_a_new_parallel_environment")

# ## PBS
class BcbioPBSEngineSetLauncher(launcher.PBSEngineSetLauncher):
    """Custom launcher handling heterogeneous clusters on SGE.
    """
    cores = traitlets.Integer(1, config=True)
    pename = traitlets.Unicode("", config=True)
    default_template = traitlets.Unicode("""#PBS -V
#PBS -j oe
#PBS -S /bin/sh
#PBS -q {queue}
#PBS -N bcbio-ipengine
#PBS -t 1-{n}
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
"""% (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
      ' '.join(timeout_params)))

    def start(self, n):
        self.context["cores"] = self.cores
        self.context["pename"] = str(self.pename)
        return super(BcbioPBSEngineSetLauncher, self).start(n)


class BcbioPBSControllerLauncher(launcher.PBSControllerLauncher):
    default_template = traitlets.Unicode(u"""#PBS -V
#PBS -S /bin/sh
#PBS -N ipcontroller
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
"""%(' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
     ' '.join(controller_params)))

    def start(self):
        return super(BcbioPBSControllerLauncher, self).start()

# ## Torque
class TORQUELauncher(launcher.BatchSystemLauncher):
    """A BatchSystemLauncher subclass for PBS."""

    submit_command = List(['qsub'], config=True,
        help="The PBS submit command ['qsub']")
    delete_command = List(['qdel'], config=True,
        help="The PBS delete command ['qsub']")
    job_id_regexp = CRegExp(r'\d+(\[\])?', config=True,
        help="Regular expresion for identifying the job ID [r'\d+']")

    batch_file = Unicode(u'')
    #job_array_regexp = CRegExp('#PBS\W+-t\W+[\w\d\-\$]+')
    #job_array_template = Unicode('#PBS -t 1-{n}')
    queue_regexp = CRegExp('#PBS\W+-q\W+\$?\w+')
    queue_template = Unicode('#PBS -q {queue}')

class BcbioTORQUEEngineSetLauncher(TORQUELauncher, launcher.BatchClusterAppMixin):
    """Launch Engines using PBS"""
    cores = traitlets.Integer(1, config=True)
    batch_file_name = Unicode(u'torque_engines', config=True,
        help="batch file name for the engine(s) job.")
    default_template= Unicode(u"""#!/bin/sh
#PBS -V
#PBS -j oe
#PBS -N ipengine
#PBS -t 1-{n}
#PBS -l nodes=1:ppn={cores}
#PBS -l walltime=239:00:00
%s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
"""%(' '.join(map(pipes.quote,launcher.ipengine_cmd_argv))))

    def start(self, n):
        """Start n engines by profile or profile_dir."""
        self.context["cores"] = self.cores
        return super(BcbioTORQUEEngineSetLauncher, self).start(n)

class BcbioTORQUEControllerLauncher(TORQUELauncher, launcher.BatchClusterAppMixin):
    """Launch a controller using PBS."""

    batch_file_name = Unicode(u'torque_controller', config=True,
        help="batch file name for the controller job.")
    default_template= Unicode("""#!/bin/sh
#PBS -V
#PBS -N ipcontroller
#PBS -j oe
#PBS -l walltime=239:00:00
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
"""%(' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
     ' '.join(controller_params)))

    def start(self):
        """Start the controller by profile or profile_dir."""
        return super(BcbioTORQUEControllerLauncher, self).start(1)

# ## PBSPro
class PBSPROLauncher(launcher.PBSLauncher):
    """A BatchSystemLauncher subclass for PBSPro."""
    job_array_regexp = CRegExp('#PBS\W+-J\W+[\w\d\-\$]+')
    job_array_template = Unicode('#PBS -J 1-{n}')

class BcbioPBSPROEngineSetLauncher(PBSPROLauncher, launcher.BatchClusterAppMixin):
    """Launch Engines using PBSPro"""
    batch_file_name = Unicode(u'pbspro_engines', config=True,
        help="batch file name for the engine(s) job.")
    default_template= Unicode(u"""#!/bin/sh
#PBS -V
#PBS -N ipengine
%s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
"""%(' '.join(map(pipes.quote,launcher.ipengine_cmd_argv))))

    def start(self, n):
        """Start n engines by profile or profile_dir."""
        return super(BcbioPBSPROEngineSetLauncher, self).start(n)

class BcbioPBSPROControllerLauncher(PBSPROLauncher, launcher.BatchClusterAppMixin):
    """Launch a controller using PBSPro."""

    batch_file_name = Unicode(u'pbspro_controller', config=True,
        help="batch file name for the controller job.")
    default_template= Unicode("""#!/bin/sh
#PBS -V
#PBS -N ipcontroller
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
"""%(' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
     ' '.join(controller_params)))

    def start(self):
        """Start the controller by profile or profile_dir."""
        return super(BcbioPBSPROControllerLauncher, self).start(1)

def _start(scheduler, profile, queue, num_jobs, cores_per_job):
    """Starts cluster from commandline.
    """
    ns = "cluster_helper.cluster"
    scheduler = scheduler.upper()
    engine_class = "Bcbio%sEngineSetLauncher" % scheduler
    controller_class = "Bcbio%sControllerLauncher" % scheduler
    args = launcher.ipcluster_cmd_argv + \
        ["start",
         "--daemonize=True",
         "--IPClusterEngines.early_shutdown=240",
         "--delay=10",
         "--log-level=%s" % "WARN",
         "--profile=%s" % profile,
         "--n=%s" % num_jobs,
         "--%s.cores=%s" % (engine_class, cores_per_job),
         "--IPClusterStart.controller_launcher_class=%s.%s" % (ns, controller_class),
         "--IPClusterStart.engine_launcher_class=%s.%s" % (ns, engine_class),
         "--%sLauncher.queue='%s'" % (scheduler, queue)
         ]
    if scheduler in ["SGE"]:
        args += ["--%s.pename=%s" % (engine_class, _find_parallel_environment())]

    subprocess.check_call(args)

def _stop(profile):
    subprocess.check_call(launcher.ipcluster_cmd_argv +
                          ["stop", "--profile=%s" % profile])

def _is_up(profile, n):
    try:
        client = Client(profile=profile)
        up = len(client.ids)
        client.close()
    except IOError, msg:
        return False
    else:
        return up >= n

@contextlib.contextmanager
def cluster_view(scheduler, queue, num_jobs, cores_per_job=1, profile=None):
    """Provide a view on an ipython cluster for processing.

    parallel is a dictionary with:
      - scheduler: The type of cluster to start (lsf, sge).
      - num_jobs: Number of jobs to start.
      - cores_per_job: The number of cores to use for each job.
    """
    delay = 10
    max_delay = 960
    max_tries = 10
    if profile is None:
        has_throwaway = True
        profile = create_throwaway_profile()
    else:
        has_throwaway = False
    num_tries = 0
    while 1:
        try:
            _start(scheduler, profile, queue, num_jobs, cores_per_job)
            break
        except subprocess.CalledProcessError:
            if num_tries > max_tries:
                raise
            num_tries += 1
            time.sleep(delay)
    try:
        client = None
        slept = 0
        while not _is_up(profile, num_jobs):
            time.sleep(delay)
            slept += delay
            if slept > max_delay:
                raise IOError("Cluster startup timed out.")
        #client = Client(profile=profile, cluster_id=cluster_id)
        client = Client(profile=profile)
        # push config to all engines and force them to set up logging
        #client[:]['config'] = config
        #client[:].execute('from bcbio.log import setup_logging')
        #client[:].execute('setup_logging(config)')
        #client[:].execute('from bcbio.log import logger')
        yield _get_balanced_blocked_view(client)
    finally:
        if client:
            client.close()
        _stop(profile)
        if has_throwaway:
            delete_profile(profile)

def _get_balanced_blocked_view(client):
    view = client.load_balanced_view()
    view.block = True
    return view

# ## Temporary profile management

def create_throwaway_profile():
    profile = str(uuid.uuid1())
    cmd = "ipython profile create {0} --parallel".format(profile)
    subprocess.check_call(cmd, shell=True)
    return profile

def delete_profile(profile):
    MAX_TRIES = 10
    proc = subprocess.Popen("ipython locate", stdout=subprocess.PIPE, shell=True)
    ipython_dir = proc.stdout.read().strip()
    profile_dir = "profile_{0}".format(profile)
    dir_to_remove = os.path.join(ipython_dir, profile_dir)
    if os.path.exists(dir_to_remove):
        num_tries = 0
        while True:
            try:
                shutil.rmtree(dir_to_remove)
                break
            except OSError:
                if num_tries > MAX_TRIES:
                    raise
                time.sleep(5)
                num_tries += 1
    else:
        raise ValueError("Cannot find {0} to remove, "
                         "something is wrong.".format(dir_to_remove))
