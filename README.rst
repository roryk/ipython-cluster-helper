ipython-cluster-helper
======================

Quickly and easily parallelize Python functions using IPython on a
cluster, supporting multiple schedulers. Optimizes IPython defaults to
handle larger clusters and simultaneous processes.

Example
-------

Lets say you wrote a program that takes several files in as arguments
and performs some kind of long running computation on them. Your
original implementation used a loop but it was way too slow::

    from yourmodule import long_running_function
    import sys

    if __name__ == "__main__":
        for f in sys.argv[1:]:
            long_running_function(f)

If you have access to one of the supported schedulers you can easily
parallelize your program across 5 nodes with ipython-cluster-helper::

    from cluster_helper.cluster import cluster_view
    from yourmodule import long_running_function
    import sys

    if __name__ == "__main__":
        with cluster_view(scheduler="lsf", queue="hsph", num_jobs=5) as view:
            view.map(long_running_function, sys.argv[1:])

That's it! No setup required.

How it works
------------

ipython-cluster-helper creates a throwaway parallel IPython profile,
launches a cluster and returns a view. On program exit it shuts the
cluster down and deletes the throwaway profile.

Supported schedulers
--------------------

Platform LSF ("lsf") and Sun Grid Engine ("sge"). Torque ("torque") is
functional but not fully tested yet. More to come.

Credits
-------

The cool parts of this were ripped from `bcbio-nextgen`_.

.. _bcbio-nextgen: https://github.com/chapmanb/bcbio-nextgen
