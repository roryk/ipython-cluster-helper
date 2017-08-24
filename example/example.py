import argparse
import time
import imp
import sys
from ipyparallel import require
from cluster_helper.cluster import cluster_view


def long_computation(x, y, z):
    import time
    import socket
    time.sleep(1)
    return (socket.gethostname(), x + y + z)


@require("cluster_helper")
def require_test(x):
    return True


def context_test(x):
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="example script for doing parallel work with IPython.")
    parser.add_argument(
        "--scheduler", dest='scheduler', default="",
        help="scheduler to use (lsf, sge, torque, slurm, or pbs)")
    parser.add_argument("--queue", dest='queue', default="",
                        help="queue to use on scheduler.")
    parser.add_argument("--local_controller", dest='local_controller', default=False,
                        help="run controller locally", action="store_true")
    parser.add_argument("--num_jobs", dest='num_jobs', default=3,
                        type=int, help="number of jobs to run in parallel.")
    parser.add_argument("--cores_per_job", dest="cores_per_job", default=1,
                        type=int, help="number of cores for each job.")
    parser.add_argument("--profile", dest="profile", default=None,
                        help="Optional profile to test.")
    parser.add_argument(
        "--resources", dest="resources", default=None,
        help=("Native specification flags to the scheduler, pass "
              "multiple flags using ; as a separator."))
    parser.add_argument("--timeout", dest="timeout", default=15,
                        help="Time (in minutes) to wait before timing out.")
    parser.add_argument("--memory", dest="mem", default=1,
                        help="Memory in GB to reserve.")
    parser.add_argument(
        "--local", dest="local", default=False, action="store_true")

    args = parser.parse_args()
    args.resources = {'resources': args.resources,
                      'mem': args.mem,
                      'local_controller': args.local_controller}
    if args.local:
        args.resources["run_local"] = True

    if not (args.local or (args.scheduler and args.queue)):
        print("Please specify --local to run locally or a scheduler and queue"
              "to run on with --scheduler and --queue")
        sys.exit(1)

    with cluster_view(args.scheduler, args.queue, args.num_jobs,
                      cores_per_job=args.cores_per_job,
                      start_wait=args.timeout, profile=args.profile,
                      extra_params=args.resources) as view:
        print("First check to see if we can talk to the engines.")
        results = view.map(lambda x: "hello world!", range(5))
        print("This long computation that waits for 5 seconds before "
              "returning takes a while to run serially..")
        start_time = time.time()
        results = list(map(long_computation, range(20), range(20, 40), range(40, 60)))
        print(results)
        print("That took {} seconds.".format(time.time() - start_time))
        print("Running it in parallel goes much faster...")
        start_time = time.time()
        results = list(view.map(long_computation, range(20), range(20, 40), range(40, 60)))
        print(results)
        print("That took {} seconds.".format(time.time() - start_time))

        try:
            imp.find_module('dill')
            found = True
        except ImportError:
            found = False

        if False:
            def make_closure(a):
                """make a function with a closure, and return it"""
                def has_closure(b):
                    return a * b
                return has_closure
            closure = make_closure(5)
            print("With dill installed, we can pickle closures!")
            print(closure)
            print(view.map(closure, [3]))

            with open("test", "w") as test_handle:
                print("Does context break it?")
                print(view.map(context_test, [3]))

                print("Does context break it with a closure?")
                print(view.map(closure, [3]))

            print("But wrapping functions with @reqiure is broken.")
            print(view.map(require_test, [3]))
