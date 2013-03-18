from cluster_helper.cluster import cluster_view
import argparse
import time


def long_computation(x, y, z):
    import time
    time.sleep(5)
    return x + y + z

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="example script for doing parallel "
                                     "work with IPython.")
    parser.add_argument("--scheduler", dest='scheduler', required=True,
                        help="scheduler to use (lsf, sge, torque or pbs)")
    parser.add_argument("--queue", dest='queue', required=True,
                        help="queue to use on scheduler.")
    parser.add_argument("--num_jobs", dest='num_jobs', required=True,
                        type=int, help="number of jobs to run in parallel.")
    parser.add_argument("--cores_per_job", dest="cores_per_job", default=1,
                        type=int, help="number of cores for each job.")

    args = parser.parse_args()

    with cluster_view(args.scheduler, args.queue, args.num_jobs) as view:
        print "First check to see if we can talk to the engines."
        results = view.map(lambda x: "hello world!", range(5))
        print ("This long computation that waits for 5 seconds before returning "
               "takes a while to run serially..")
        start_time = time.time()
        results = map(long_computation, [1, 2, 3], [4, 5, 6], [7, 8, 9])
        print results
        print "That took {0} seconds.".format(time.time() - start_time)

        print "Running it in parallel goes much faster..."
        start_time = time.time()
        results = view.map(long_computation, [1, 2, 3], [4, 5, 6], [7, 8, 9])
        print results
        print "That took {0} seconds.".format(time.time() - start_time)
