from cluster_helper import cluster_view

if __name__ == "__main__":
    with cluster_view(scheduler="lsf", queue="hsph", num_jobs=5) as view:
        results = view.map(lambda x: "hello world!", range(5))
        print results
