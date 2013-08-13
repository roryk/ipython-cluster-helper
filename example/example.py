from cluster_helper.cluster import cluster_view
import argparse
import time


def long_computation(x, y, z):
    import time
    import socket
    time.sleep(1)
    return (socket.gethostname(), x + y + z)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="example script for doing parallel "
                                     "work with IPython.")
    parser.add_argument("--scheduler", dest='scheduler', required=True,
                        help="scheduler to use (lsf, sge, torque, slurm, or pbs)")
    parser.add_argument("--queue", dest='queue', required=True,
                        help="queue to use on scheduler.")
    parser.add_argument("--num_jobs", dest='num_jobs', required=True,
                        type=int, help="number of jobs to run in parallel.")
    parser.add_argument("--cores_per_job", dest="cores_per_job", default=1,
                        type=int, help="number of cores for each job.")
    parser.add_argument("--profile", dest="profile", default=None,
                        help="Optional profile to test.")
    parser.add_argument("--resources", dest="resources", default=None,
                        help="Native specification flags to the scheduler")

    args = parser.parse_args()
    args.resources = {'resources': args.resources}

    with cluster_view(args.scheduler, args.queue, args.num_jobs,
                      profile=args.profile, extra_params=args.resources) as view:
        print "First check to see if we can talk to the engines."
        results = view.map(lambda x: "hello world!", range(5))
        print ("This long computation that waits for 5 seconds before returning "
               "takes a while to run serially..")
        start_time = time.time()
        results = map(long_computation, range(20), range(20, 40), range(40, 60))
        print results
        print "That took {0} seconds.".format(time.time() - start_time)

        print "Running it in parallel goes much faster..."
        start_time = time.time()
        results = view.map(long_computation, range(20), range(20, 40), range(40, 60))
        print results
        print "That took {0} seconds.".format(time.time() - start_time)
