"""Generate node runtime feature out of time series."""

import pprint as pp

from typing import Dict, Callable
from collections import defaultdict
from oracle.feature.utils import sequence_summary


_null_feature_gate = {
}


def gen_features(entries: list, metadata: dict, post_proc: Callable = lambda x: x, post_proc_args=None) -> Dict:
    """Given a list of log entries, return the feature vector. Each entry contains the metadata
    about the metric and its timeseries. The resulting feature vector is indexed by
    the pod name and then the container name.

    If the counter is an accumulator, return its last value, else return a summary for the timeseries.
    TODO: this is now tightly coupled with spark
    """

    feature_vec = dict()
    exec_features = defaultdict(list)  # need to be summarized before dump to feature_vec

    print("node feature: total entries", len(entries))

    info = metadata["info"]

    pod_node_map = metadata["pod_node_map"]
    driver_nodes, exec_nodes = set(), set()
    for k, v in pod_node_map.items():
        if "driver" in k:
            driver_nodes.add(v)
        else:
            exec_nodes.add(v)

    for e in entries:
        metric = e["metric"]
        values = e["values"]

        name: str = metric["__name__"]

        node_ip = metric["instance"].split(":")[0]
        node_name = info[node_ip]["name"]
        node_role = info[node_ip]["role"]

        if node_role == "master" or name in _null_feature_gate:
            continue

        if node_name in driver_nodes:
            feature_vec["_".join(["nf", "driver", name])] = float(values[0][1])
        elif node_name in exec_nodes:
            exec_features["_".join(["nf", "exec", name])].append((values[0][0], float(values[0][1])))

    # inject the executor features
    for k, v in exec_features.items():
        tss = sequence_summary(v)
        for i, j in tss.items():
            feature_vec["_".join([k, i])] = float(j)
    print("node feature: feature vector", len(feature_vec))

    return post_proc(feature_vec)



