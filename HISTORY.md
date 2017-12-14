## 0.6.0 (14 Dec 2017)

- Stop IPython parallel trying to setup .ipython in home directories during runs by
  setting IPYTHONDIR. This avoids intermittent InvalidSignature errors.

## 0.5.9 (16 Nov 2017)

- PBSPro: Pass memory default to controller and make configurable with `conmem`,
  duplicating SLURM functionality. Thanks to Oliver Hofmann.

## 0.5.8 (18 Oct 2017)

- Fix SLURM resource specifications to avoid missing directives in the case of
  empty lines in specifications.

## 0.5.7 (11 Oct 2017)

- Fix for SLURM engine output files not having the proper job IDs.
- Additional fixes for python3 support.

## 0.5.6 (2 Aug 2017)

- Additional work to better resolve local IP addresses, first trying to pick
  an IP from the fully qualified domain name if available. Thanks to Linh Vu.
- Support PBSPro systems without select statement enabled. Thanks to Roman Valls
  Guimerà.
- Enable python3 support for Slurm. Thanks to Vlad Saveliev.
- Enable support for ipython 5.

## 0.5.5 (29 May 2017)

- Try to better resolve multiple local addresses for controllers. Prioritize
  earlier addresses (eth0) over later (eth1) if all are valid. Thanks to Luca Beltrame.
- Provide separate logging of SLURM engine job arrays. Thanks to Oliver Hofmann.
- Set minimum PBSPro controller memory usage to 1GB. Thanks to Guus van Dalum
  @vandalum.

## 0.5.4 (24 February 2017)
- Ensure /bin/sh used for Torque/PBSPro submissions to prevent overwriting
  environment from bashrc. Pass LD_* exports, which are also filtered by Torque.
  Thanks to Andrey Tovchigrechko.
- Add option to run controller locally. Thanks to Brent Pederson (@brentp) and
  Sven-Eric Schelhorn (@schelhorn).
- Fix for Python 3 compatibility in setup.py. Thanks to Amar Chaudhari
  (@Amar-Chaudhari).

## 0.5.3 (23 August 2016)

- Emit warning message instead of failing when we try to shut down a cluster and
  it is already shut down. Thanks to Gabriel F. Berriz (@gberriz) for raising
  the issue.
- Python 2/3 compatibility. Thanks to Alain Péteut (@peteut) and Matt De Both
  (@mdeboth).
- Pass LD_PRELOAD to bcbio SGE worker jobs to enable compatibility with
  PetaGene.

## 0.5.2 (8 June 2016)

- Pin requirements to use IPython < 5.0 until we migrate to new release.
- Fix passing optional resources to PBSPro. Thanks to Endre Sebestyén.
- Spin up individual engines instead of job arrays for PBSPro clusters. Thanks to
  Thanks to Endre Sebestyen (@razZ0r), Francesco Ferrari and Tiziana Castrignanò
  for raising the issue, providing an account with access to PBSPro on the Cineca
  cluster and testing that the fix works.
- Remove sleep command to stagger engine startups in SGE which breaks if bc
  command not present. Thanks to Juan Caballero.

## 0.5.1 (January 25, 2016)
- Add support for UGE (an open-source fork of SGE) Thanks to Andrew Oler.

## 0.5.0 (October 8, 2015)

- Adjust memory resource fixes for LSF to avoid underscheduling memory on other clusters
- Change timeouts to provide faster startup for IPython clusters (thanks to Maciej Wójcikowski)
- Don't send SIGKILL to SLURM job arrays since it often doesn't kill all of the jobs. (@mwojcikowski)

## 0.4.9 (August 17, 2015)

- Fix memory resources for LSF when jobs requests 1 core and mincore has a value > 1.

## 0.4.8 (August 15, 2015)

- Fix tarball issue, ensuring requirements.txt included.

## 0.4.7 (August 15, 2015)

- Support for IPython 4.0 (@brainstorm, @roryk, @chapmanb, @lpantano)

## 0.4.6 (July 22, 2015)
- Support `numengines` for Torque and PBSPro for correct parallelization with bcbio-nextgen.
- Ensure we only install IPython < 4.0 since 4.0 development versions split out IPython parallel
  into a new namespace. Will support this when it is officially released.
- Added wait_for_all_engines support to return the view only after all engines are up (@matthias-k)
- IPython.parallel is moving towards separate packages (@brainstorm).

## 0.4.5 (May 20, 2015)
- Added --local and --cores-per-job support to the example script (@matthias-k)
- Add ability to get a cluster view without the context manager (@matthias-k)

## 0.4.4 (April 23, 2014)
- Python 3 compatibility (@mjdellwo)

## 0.4.3 (March 18, 2015)

- Fix resource specification problem for SGE. Thanks to Zhengqiu Cai.

## 0.4.2 (March 7, 2015)

- Additional IPython preparation cleanups to prevent race conditions. Thanks to
  Lorena Pantano.

## 0.4.1 (March 6, 2015)

- Pre-create IPython database directory to prevent race conditions on
  new filesystems with a shared home directory, like AWS.

## 0.4.0 (February 17, 2015)

- Support `conmem` resource parameter, which enables control over the memory
  supplied to the controller for SLURM runs.

## 0.3.7 (December 09, 2014)

- Fix SLURM installations that has accounts but the user is not listed.

## 0.3.6 (October 18, 2014)

- Support SLURM installations that do not use accounts, skip finding and passing
  an account flag.

## 0.3.5 (October 12, 2014)

- Fix `minconcores` specification to be passed correctly to controllers.

## 0.3.4 (October 8, 2014)

- Support `mincores` resource specification for SGE.
- Add `minconcores` resource specification for specifying if controllers should
  use more than a single core, instead of coupling this to `mincores`. Enables
  running on systems with only minimum requirements for core usage.

## 0.3.3 (September 15, 2014)

- Handle mincore specification for multicore jobs when memory limits cores to
  less than the `mincores` amount.
- Improve timeouts for running on interruptible queues to avoid engine failures
  when controllers are requeued.

## 0.3.2 (September 10, 2104)

- Support PBSPro based on Torque setup. Thanks to Piet Jones.
- Improve ability to run on interruptible queuing systems by increasing timeout
  when killing non-responsive engines from a controller. Now 1 hour instead of 3
  minutes, allowing requeueing of engines.

## 0.3.1 (August 20, 2014)

- Add a special resource flag `-r mincores=n` which requires single core jobs to
  use at least n cores. Useful for shared queues where we can only run multicore
  jobs, and for sharing memory usage across multiple cores for programs with
  spiky memory utilization like variant calling. Available on LSF and SLURM for
  testing.
- Add hook to enable improved cleanup of controllers/engines from bcbio-nextgen.

## 0.3.0 (August 6, 2014)

- Make a cluster available after a single engine has registered. Avoids need to
  wait for entire cluster to become available on busy clusters.
- Change default SLURM time limit to 1 day to avoid asking for excessive
  resources. This can be overridden by passing `-r` resource with desired runtime.

## 0.2.19 (April 28, 2014)
- Respect RESOURCE_RESERVE_PER_SLOT for LSF. This causes resources to be specified
  at the level of a core instead of a job

## 0.2.18 (April 17, 2014)

- Added ability to get direct view to a cluster.
- Use rusage for LSF jobs instead of --mem. This walls of the memory, preventing nodes
  from being oversubscribed.
