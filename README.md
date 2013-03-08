#ipython-cluster-helper

Tool to easily fire up an IPython cluster and obtain a view on different schedulers.

## Example

    from cluster_helper import cluster_view

    if __name__ == "__main__":
        with cluster_view(scheduler="lsf", queue="hsph", num_jobs=5) as view:
            results = view.map(lambda x: "hello world!", range(5))
            print results
            
## How it works
It creates a throwaway IPython profile, launches a cluster and returns a view. On program exit it
shuts the cluster down.

## Supported schedulers
Platform LSF and Sun Grid Engine.

## Credits
The cool parts of this were ripped from [bcbio-nextgen](https://github.com/chapmanb/bcbio-nextgen).
