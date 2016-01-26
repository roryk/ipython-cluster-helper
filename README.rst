ipython-cluster-helper
======================
.. image:: https://travis-ci.org/roryk/ipython-cluster-helper.svg
    :target: https://travis-ci.org/roryk/ipython-cluster-helper
.. image:: https://zenodo.org/badge/3658/roryk/ipython-cluster-helper.svg
    :target: https://zenodo.org/record/17119#.VT7oWs5dqyM

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

To run a local cluster for testing purposes pass `run_local` as an extra
parameter to the cluster_view function::

    with cluster_view(scheduler=None, queue=None, num_jobs=5,
                      extra_params={"run_local": True}) as view:
        view.map(long_running_function, sys.argv[1:])

How it works
------------

ipython-cluster-helper creates a throwaway parallel IPython profile,
launches a cluster and returns a view. On program exit it shuts the
cluster down and deletes the throwaway profile.

Supported schedulers
--------------------

Platform LSF ("lsf"), Sun Grid Engine ("sge"), Torque ("torque"), SLURM ("slurm").

Credits
-------

The cool parts of this were ripped from `bcbio-nextgen`_.

Contributors
------------
* Brad Chapman (@chapmanb)
* Mario Giovacchini (@mariogiov)
* Valentine Svensson (@vals)
* Roman Valls (@brainstorm)
* Rory Kirchner (@roryk)
* Luca Beltrame (@lbeltrame)
* James Porter (@porterjamesj)
* Billy Ziege (@billyziege)
* ink1 (@ink1)
* @mjdellwo
* @matthias-k
* Andrew Oler (@oleraj)


.. _bcbio-nextgen: https://github.com/chapmanb/bcbio-nextgen
