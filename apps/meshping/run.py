import ray
from collections import defaultdict

from apps.net.ping import pairwise_lat


@ray.remote
def run(run_config: dict, wrks: dict) -> dict:
    lats = pairwise_lat([i for _, v in wrks.items() for i in v],
                        map_dst=True)
    r = defaultdict(list)

    for k, v in lats.items():
        src, dst = k
        lat = v
        r["src"].append(src)
        r["dst"].append(dst)
        r["lat"].append(lat)

    # make same number of rows
    for k, v in run_config.items():
        for _ in range(len(lats)):
            r[k].append(v)
    return r
