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
