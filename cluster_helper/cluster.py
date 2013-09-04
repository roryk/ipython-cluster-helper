"""Distributed execution using an IPython cluster.

Uses IPython parallel to setup a cluster and manage execution:

http://ipython.org/ipython-doc/stable/parallel/index.html
Borrowed from Brad Chapman's implementation:
https://github.com/chapmanb/bcbio-nextgen/blob/master/bcbio/distributed/ipython.py
"""
import contextlib
import math
import os
import pipes
import uuid
import shutil
import subprocess
import time
from distutils.version import LooseVersion
import sys

from IPython.parallel import Client
from IPython.parallel.apps import launcher
from IPython.parallel import error as iperror
from IPython.utils import traitlets
from IPython.utils.traitlets import (List, Unicode, CRegExp)

# ## Custom launchers

# Handles longer timeouts for startup and shutdown
# with pings between engine and controller.
# Update the ping period to 15 seconds instead of 3.
# Shutdown engines if they cannot be contacted for 3 minutes
# from controller.
# Makes engine pingback shutdown higher, since this is
# not consecutive misses.
timeout_params = ["--timeout=60", "--IPEngineApp.wait_for_url_file=960",
                  "--EngineFactory.max_heartbeat_misses=100"]
controller_params = ["--nodb", "--hwm=1", "--scheme=lru",
                     "--HeartMonitor.max_heartmonitor_misses=12",
                     "--HeartMonitor.period=16000"]


# ## Platform LSF
class BcbioLSFEngineSetLauncher(launcher.LSFEngineSetLauncher):
    """Custom launcher handling heterogeneous clusters on LSF.
    """
    batch_file_name = Unicode(unicode("lsf_engine" + str(uuid.uuid4())))
    cores = traitlets.Integer(1, config=True)
    mem = traitlets.Unicode("", config=True)
    default_template = traitlets.Unicode("""#!/bin/sh
#BSUB -q {queue}
#BSUB -J bcbio-ipengine[1-{n}]
#BSUB -oo bcbio-ipengine.bsub.%%J
#BSUB -n {cores}
#BSUB -R "span[hosts=1]"
{mem}
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
    """ % (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
           ' '.join(timeout_params)))

    def start(self, n):
        self.context["cores"] = self.cores
        if self.mem:
            # scale memory to kb
            mem = int(float(self.mem) * 1024.0 * 1024.0)
            self.context["mem"] = "#BSUB -M %s" % mem
        else:
            self.context["mem"] = ""
        return super(BcbioLSFEngineSetLauncher, self).start(n)

class BcbioLSFControllerLauncher(launcher.LSFControllerLauncher):
    batch_file_name = Unicode(unicode("lsf_controller" + str(uuid.uuid4())))
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
    batch_file_name = Unicode(unicode("sge_engine" + str(uuid.uuid4())))
    cores = traitlets.Integer(1, config=True)
    pename = traitlets.Unicode("", config=True)
    resources = traitlets.Unicode("", config=True)
    mem = traitlets.Unicode("", config=True)
    default_template = traitlets.Unicode("""#$ -V
#$ -cwd
#$ -b y
#$ -j y
#$ -S /bin/sh
#$ -q {queue}
#$ -N bcbio-ipengine
#$ -t 1-{n}
#$ -pe {pename} {cores}
{mem}
{resources}
echo \($SGE_TASK_ID - 1\) \* 0.5 | bc | xargs sleep
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
"""% (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
      ' '.join(timeout_params)))

    def start(self, n):
        self.context["cores"] = self.cores
        if self.mem:
            self.context["mem"] = "#$ -l mem_free=%sG" % self.mem
        else:
            self.context["mem"] = ""
        self.context["pename"] = str(self.pename)
        self.context["resources"] = "\n".join(["#$ -l %s" % r.strip()
                                               for r in str(self.resources).split(";")
                                               if r.strip()])
        return super(BcbioSGEEngineSetLauncher, self).start(n)

class BcbioSGEControllerLauncher(launcher.SGEControllerLauncher):
    batch_file_name = Unicode(unicode("sge_controller" + str(uuid.uuid4())))
    default_template = traitlets.Unicode(u"""#$ -V
#$ -S /bin/sh
#$ -cwd
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
                if _has_parallel_environment(line):
                    return name
    raise ValueError("Could not find an SGE environment configured for parallel execution. " \
                     "See %s for SGE setup instructions." %
                     "https://blogs.oracle.com/templedf/entry/configuring_a_new_parallel_environment")


def _has_parallel_environment(line):
    if line.startswith("allocation_rule"):
        if line.find("$pe_slots") >= 0 or line.find("$fill_up") >= 0:
                return True
    return False


# ## SLURM
class SLURMLauncher(launcher.BatchSystemLauncher):
    """A BatchSystemLauncher subclass for SLURM
    """
    submit_command = List(['sbatch'], config=True,
        help="The SLURM submit command ['sbatch']")
    # Send SIGKILL instead of term, otherwise the job is "CANCELLED", not
    # "FINISHED"
    delete_command = List(['scancel', '--signal=KILL'], config=True,
        help="The SLURM delete command ['scancel']")
    job_id_regexp = CRegExp(r'\d+', config=True,
        help="A regular expression used to get the job id from the output of 'sbatch'")

    batch_file = Unicode(u'', config=True,
        help="The string that is the batch script template itself.")

    queue_regexp = CRegExp('#SBATCH\W+-p\W+\w')
    queue_template = Unicode('#SBATCH -p {queue}')


class BcbioSLURMEngineSetLauncher(SLURMLauncher, launcher.BatchClusterAppMixin):
    """Custom launcher handling heterogeneous clusters on SLURM
    """
    batch_file_name = Unicode(unicode("SLURM_engine" + str(uuid.uuid4())))
    cores = traitlets.Integer(1, config=True)
    mem = traitlets.Unicode("", config=True)
    default_template = traitlets.Unicode("""#!/bin/sh
#SBATCH -p {queue}
#SBATCH -J bcbio-ipengine[1-{n}]
#SBATCH -o bcbio-ipengine.out.%%j
#SBATCH -e bcbio-ipengine.err.%%j
#SBATCH --cpus-per-task={cores}
#SBATCH --array=1-{n}
{mem}
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
    """ % (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
           ' '.join(timeout_params)))

    def start(self, n):
        self.context["cores"] = self.cores
        if self.mem:
            # scale memory to Mb and divide by cores
            mem = int(math.ceil(float(self.mem) * 1024.0 / self.cores))
            self.context["mem"] = "#SBATCH --mem-per-cpu=%s" % mem
        else:
            self.context["mem"] = ""
        return super(BcbioSLURMEngineSetLauncher, self).start(n)

class BcbioSLURMControllerLauncher(SLURMLauncher, launcher.BatchClusterAppMixin):
    batch_file_name = Unicode(unicode("SLURM_controller" + str(uuid.uuid4())))
    default_template = traitlets.Unicode("""#!/bin/sh
#SBATCH -J bcbio-ipcontroller
#SBATCH -o bcbio-ipcontroller.out.%%j
#SBATCH -e bcbio-ipcontroller.err.%%j
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
    """%(' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
         ' '.join(controller_params)))
    def start(self):
        return super(BcbioSLURMControllerLauncher, self).start(1)


class BcbioOLDSLURMEngineSetLauncher(SLURMLauncher, launcher.BatchClusterAppMixin):
    """Launch engines using SLURM for version < 2.6"""
    machines = traitlets.Integer(1, config=True)
    account = traitlets.Unicode("", config=True)
    timelimit = traitlets.Unicode("", config=True)
    batch_file_name = Unicode(unicode("SLURM_engines" + str(uuid.uuid4())),
                              config=True, help="batch file name for the engine(s) job.")

    default_template = Unicode(u"""#!/bin/sh
#SBATCH -A {account}
#SBATCH --job-name ipengine
#SBATCH -N {machines}
#SBATCH -t {timelimit}
srun -N {machines} -n {n} %s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
    """ % (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
           ' '.join(timeout_params)))

    def start(self, n):
        """Start n engines by profile or profile_dir."""
        self.context["machines"] = self.machines
        self.context["account"] = self.account
        self.context["timelimit"] = self.timelimit
        return super(BcbioSLURMEngineSetLauncher, self).start(n)


class BcbioOLDSLURMControllerLauncher(SLURMLauncher, launcher.BatchClusterAppMixin):
    """Launch a controller using SLURM for versions < 2.6"""
    account = traitlets.Unicode("", config=True)
    timelimit = traitlets.Unicode("", config=True)
    batch_file_name = Unicode(unicode("SLURM_controller" + str(uuid.uuid4())),
                              config=True, help="batch file name for the engine(s) job.")

    default_template = Unicode("""#!/bin/sh
#SBATCH -A {account}
#SBATCH --job-name ipcontroller
#SBATCH -t {timelimit}
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
""" % (' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
     ' '.join(controller_params)))

    def start(self):
        """Start the controller by profile or profile_dir."""
        self.context["account"] = self.account
        self.context["timelimit"] = self.timelimit
        return super(BcbioSLURMControllerLauncher, self).start(1)


# ## PBS
class BcbioPBSEngineSetLauncher(launcher.PBSEngineSetLauncher):
    """Custom launcher handling heterogeneous clusters on SGE.
    """
    batch_file_name = Unicode(unicode("pbs_engines" + str(uuid.uuid4())))
    cores = traitlets.Integer(1, config=True)
    mem = traitlets.Unicode("", config=True)
    pename = traitlets.Unicode("", config=True)
    resources = traitlets.Unicode("", config=True)
    default_template = traitlets.Unicode("""#PBS -V
#PBS -j oe
#PBS -S /bin/sh
#PBS -q {queue}
#PBS -N bcbio-ipengine
#PBS -t 1-{n}
{mem}
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
"""% (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
      ' '.join(timeout_params)))

    def start(self, n):
        self.context["cores"] = self.cores
        self.context["pename"] = str(self.pename)
        if self.mem:
            self.context["mem"] = "#PBS -l mem=%sgb" % self.mem
        else:
            self.context["mem"] = ""
        return super(BcbioPBSEngineSetLauncher, self).start(n)


class BcbioPBSControllerLauncher(launcher.PBSControllerLauncher):
    batch_file_name = Unicode(unicode("pbs_controller" + str(uuid.uuid4())))
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
    mem = traitlets.Unicode("", config=True)
    batch_file_name = Unicode(unicode("torque_engines" + str(uuid.uuid4())),
                              config=True, help="batch file name for the engine(s) job.")
    default_template = Unicode(u"""#!/bin/sh
#PBS -V
#PBS -j oe
#PBS -N ipengine
#PBS -t 1-{n}
#PBS -l nodes=1:ppn={cores}
{mem}
#PBS -l walltime=239:00:00
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
    """ % (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
           ' '.join(timeout_params)))

    def start(self, n):
        """Start n engines by profile or profile_dir."""
        self.context["cores"] = self.cores
        if self.mem:
            self.context["mem"] = "#PBS -l mem=%sgb" % self.mem
        else:
            self.context["mem"] = ""
        return super(BcbioTORQUEEngineSetLauncher, self).start(n)


class BcbioTORQUEControllerLauncher(TORQUELauncher, launcher.BatchClusterAppMixin):
    """Launch a controller using PBS."""
    batch_file_name = Unicode(unicode("torque_controller" + str(uuid.uuid4())),
                              config=True, help="batch file name for the engine(s) job.")

    default_template = Unicode("""#!/bin/sh
#PBS -V
#PBS -N ipcontroller
#PBS -j oe
#PBS -l walltime=239:00:00
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
""" % (' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
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
    default_template = Unicode(u"""#!/bin/sh
#PBS -V
#PBS -N ipengine
%s %s --profile-dir="{profile_dir}" --cluster-id="{cluster_id}"
    """ % (' '.join(map(pipes.quote, launcher.ipengine_cmd_argv)),
           ' '.join(timeout_params)))

    def start(self, n):
        """Start n engines by profile or profile_dir."""
        return super(BcbioPBSPROEngineSetLauncher, self).start(n)


class BcbioPBSPROControllerLauncher(PBSPROLauncher, launcher.BatchClusterAppMixin):
    """Launch a controller using PBSPro."""

    batch_file_name = Unicode(u'pbspro_controller', config=True,
        help="batch file name for the controller job.")
    default_template = Unicode("""#!/bin/sh
#PBS -V
#PBS -N ipcontroller
%s --ip=* --log-to-file --profile-dir="{profile_dir}" --cluster-id="{cluster_id}" %s
""" % (' '.join(map(pipes.quote, launcher.ipcontroller_cmd_argv)),
     ' '.join(controller_params)))

    def start(self):
        """Start the controller by profile or profile_dir."""
        return super(BcbioPBSPROControllerLauncher, self).start(1)


def _get_profile_args(profile):
    if os.path.isdir(profile) and os.path.isabs(profile):
        return ["--profile-dir=%s" % profile]
    else:
        return ["--profile=%s" % profile]


def _start(scheduler, profile, queue, num_jobs, cores_per_job, cluster_id,
           extra_params):
    """Starts cluster from commandline.
    """
    ns = "cluster_helper.cluster"
    scheduler = scheduler.upper()
    if scheduler == "SLURM" and _slurm_is_old():
        scheduler = "OLDSLURM"
    engine_class = "Bcbio%sEngineSetLauncher" % scheduler
    controller_class = "Bcbio%sControllerLauncher" % scheduler
    if not (engine_class in globals() and controller_class in globals()):
        print ("The engine and controller class %s and %s are not "
               "defined. " % (engine_class, controller_class))
        print ("This may be due to ipython-cluster-helper not supporting "
               "your scheduler. If it should, please file a bug report at "
               "http://github.com/roryk/ipython-cluster-helper. Thanks!")
        sys.exit(1)

    args = launcher.ipcluster_cmd_argv + \
        ["start",
         "--daemonize=True",
         "--IPClusterEngines.early_shutdown=240",
         "--delay=10",
         "--log-to-file",
         "--debug",
         "--n=%s" % num_jobs,
         "--%s.cores=%s" % (engine_class, cores_per_job),
         "--%s.resources=%s" % (engine_class, extra_params.get("resources", "")),
         "--%s.mem='%s'" % (engine_class, extra_params.get("mem", "")),
         "--IPClusterStart.controller_launcher_class=%s.%s" % (ns, controller_class),
         "--IPClusterStart.engine_launcher_class=%s.%s" % (ns, engine_class),
         "--%sLauncher.queue='%s'" % (scheduler, queue),
         "--cluster-id=%s" % (cluster_id)
         ]
    args += _get_profile_args(profile)
    if scheduler in ["SGE"]:
        args += ["--%s.pename=%s" % (engine_class, _find_parallel_environment())]
    elif scheduler in ["SLURM"]:
        if _slurm_is_old():
            # SLURM cannot get resource atts (native specification) as SGE does
            slurm_atrs = {}
            extra_params = extra_params['resources'].split(';')

            for parm in extra_params:
                atr = parm.split('=')
                slurm_atrs[atr[0]] = atr[1]
            extra_params = slurm_atrs

            args += ["--%s.machines=%s" % (engine_class, extra_params.get("machines", "1"))]
            args += ["--%s.account=%s" % (engine_class, extra_params["account"])]
            args += ["--%s.account=%s" % (controller_class, extra_params["account"])]
            args += ["--%s.timelimit=%s" % (engine_class, extra_params["timelimit"])]
            args += ["--%s.timelimit=%s" % (controller_class, extra_params["timelimit"])]

    subprocess.check_call(args)
    return cluster_id


def _stop(profile, cluster_id):
    args = launcher.ipcluster_cmd_argv + \
           ["stop", "--cluster-id=%s" % cluster_id]
    args += _get_profile_args(profile)
    subprocess.check_call(args)


def _is_up(url_file, n):
    try:
        client = Client(url_file, timeout=60)
        up = len(client.ids)
        client.close()
    except iperror.TimeoutError:
        return False
    except IOError:
        return False
    else:
        return up >= n


@contextlib.contextmanager
def cluster_view(scheduler, queue, num_jobs, cores_per_job=1, profile=None,
                 start_wait=16, extra_params=None, retries=None):
    """Provide a view on an ipython cluster for processing.

      - scheduler: The type of cluster to start (lsf, sge, pbs, torque).
      - num_jobs: Number of jobs to start.
      - cores_per_job: The number of cores to use for each job.
      - start_wait: How long to wait for the cluster to startup, in minutes.
        Defaults to 16 minutes. Set to longer for slow starting clusters.
      - retries: Number of retries to allow for failed tasks.
    """
    if extra_params is None:
        extra_params = {}
    delay = 10
    max_delay = start_wait * 60
    # Increase default delay without changing max_delay for back compatibility
    delay = delay * 3
    max_tries = 10
    if profile is None:
        has_throwaway = True
        profile = create_throwaway_profile()
    else:
        has_throwaway = False
    num_tries = 0

    cluster_id = str(uuid.uuid4())
    url_file = get_url_file(profile, cluster_id)

    while 1:
        try:
            _start(scheduler, profile, queue, num_jobs, cores_per_job, cluster_id, extra_params)
            break
        except subprocess.CalledProcessError:
            if num_tries > max_tries:
                raise
            num_tries += 1
            time.sleep(delay)
    try:
        client = None
        slept = 0
        while not _is_up(url_file, num_jobs):
            time.sleep(delay)
            slept += delay
            if slept > max_delay:
                raise IOError("Cluster startup timed out.")
        client = Client(url_file, timeout=60)
        yield _get_balanced_blocked_view(client, retries)
    finally:
        if client:
            client.close()
        _stop(profile, cluster_id)
        if has_throwaway:
            delete_profile(profile)


def _get_balanced_blocked_view(client, retries):
    view = client.load_balanced_view()
    view.set_flags(block=True)
    if retries:
        view.set_flags(retries=int(retries))
    return view

def _slurm_is_old():
    return LooseVersion(_slurm_version()) < LooseVersion("2.6")

def _slurm_version():
    version_line = subprocess.Popen("sinfo -V", shell=True,
                                    stdout=subprocess.PIPE).communicate()[0]
    return version_line.split()[1]

# ## Temporary profile management

def create_throwaway_profile():
    profile = str(uuid.uuid1())
    cmd = "ipython profile create {0} --parallel".format(profile)
    subprocess.check_call(cmd, shell=True)
    return profile


def get_ipython_dir(profile):
    proc = subprocess.Popen("ipython locate", stdout=subprocess.PIPE, shell=True)
    ipython_dir = proc.stdout.read().strip()
    profile_dir = "profile_{0}".format(profile)
    return os.path.join(ipython_dir, profile_dir)


def get_url_file(profile, cluster_id):
    if os.path.isdir(profile) and os.path.isabs(profile):
        ipython_dir = profile
    else:
        ipython_dir = get_ipython_dir(profile)
    security_dir = os.path.join(ipython_dir, "security")
    url_file = "ipcontroller-{0}-client.json".format(cluster_id)
    return os.path.join(security_dir, url_file)


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
