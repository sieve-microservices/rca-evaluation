import sys
import itertools

#from rq import Queue
#from redis import Redis

#from cluster import cluster_service
import cluster

from graphs import draw_graph
from grangercausality import compare_services, load_graph
import metadata

from ipyparallel import Client
rc = Client()
lview = rc.load_balanced_view()

def cluster_services(path):
    data = metadata.load(path)
    def _cluster_service(args):
        import cluster
        return cluster.cluster_service(*args)
    ids = []
    for cluster_size in range(1, 8):
        for service in data["services"]:
            res = lview.apply_async(_cluster_service, (path, service, cluster_size))
            ids.extend(res.msg_ids)
    return ids


def adfuller(paths):
    def _draw(path):
        import adfuller
        return adfuller.draw(path)
    return lview.map(_draw, paths)

def increase_cluster_size(path):
    queue = Queue(connection=Redis("jobqueue.local"))
    data = metadata.load(path)
    best_score = -1
    best = -1
    for service in data["services"]:
        for key, value in service.get("clusters", {}).items():
            score = value.get("silhouette_score", -1)
            if best_score < score:
                best = int(key)
                best_score = score
        if best in [6, 7]:
            for cluster_size in range(8, min(len(service["preprocessed_fields"]), 15)):
                queue.enqueue_call(func=cluster_service, args=(path, service, cluster_size), timeout=3600*3)

def find_causality(callgraph_path, path):
    data = metadata.load(path)
    call_pairs = load_graph(callgraph_path)
    services = {}
    for srv in data["services"]:
        services[srv["name"]] = srv
    ids = []
    def _compare_services(args):
        from grangercausality import compare_services
        compare_services(*args)
    for srv_a, srv_b in call_pairs:
        res = lview.apply_async(_compare_services, (services[srv_a], services[srv_b], path))
        ids.extend(res.msg_ids)
    return ids

def draw_graphs(paths):
    queue = Queue(connection=Redis("jobqueue.local"))
    for path in paths:
        queue.enqueue_call(func=draw_graph, args=(path, ), timeout=1000*3)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurement\n" % sys.argv[0])
        sys.exit(1)
    #paths = sys.argv[1:]
    #for i, _ in enumerate(adfuller(paths)):
    #    print("progress: %d/%d" % (i, len(paths)))

    total = 0
    ids = []
    for arg in sys.argv[1:]:
        res = find_causality("foo", arg)
        #res = increase_cluster_size(arg)
        #res = cluster_services(arg)
        ids.extend(res)
    lview.get_result(ids, owner=False).wait_interactive()

    #draw_graphs(sys.argv[1:])
