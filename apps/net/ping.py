import ray
import time

from itertools import combinations
from . import remote as rt


def pairwise_lat(nodes: list, count=5, interval=0.2, map_dst=False):
    start = time.time()
    pairs = list(combinations(nodes, 2))
    mapped_pairs = list(pairs)
    if map_dst:
        from .util import k8s
        ex_ip_to_in_ip = k8s.get_worker_external_internal_ip_map()
        mapped_pairs = [(s, ex_ip_to_in_ip[d])
                        for s, d in pairs]

    print(f"pairwise lat: pinging {len(mapped_pairs)} pairs..")
    futures = list()
    for src, dst in mapped_pairs:
        futures.append(lat_async.remote(src, dst, count, interval))

    print(f"pairwise lat: took {time.time() - start}s")
    return {p: l for p, l in zip(pairs, ray.get(futures))}


@ray.remote
def lat_async(*args):
    return lat(*args)


def bipartite_lats(part_1: list, part_2: list, debug=False):
    lats = list()
    for i, j in zip(part_1, part_2):
        lats.append(lat(i, j, debug=debug))
    return lats


def lat(src, dst, count=5, interval=0.2, debug=False):
    raw = rt.cmd_remote([src],
                        cmd_=f"ping -i {interval} -c {count} {dst}",
                        out=True)
    for l in raw[0].decode("utf-8").split("\n"):
        if debug:
            print("net.ping.lat:", l)
        if "min/avg" in l:
            # returns ms
            return float(l.split()[3].split("/")[0]) * 1000
    return -1
