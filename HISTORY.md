## 0.2.19 (in progress)
- Respect RESOURCE_RESERVE_PER_SLOT for LSF. This causes resources to be specified
  at the level of a core instead of a job

## 0.2.18 (April 17, 2014)

- Added ability to get direct view to a cluster.
- Use rusage for LSF jobs instead of --mem. This walls of the memory, preventing nodes
  from being oversubscribed.
