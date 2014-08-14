## 0.3.1 (in progress)

- Add a special resource flag `-r mincores=n` which requires single core jobs to
  use at least n cores. Useful for shared queues where we can only run multicore
  jobs, and for sharing memory usage across multiple cores for programs with
  spiky memory utilization like variant calling.

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
