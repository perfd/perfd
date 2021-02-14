"""Generate container feature out of time series."""
from typing import Dict, Callable

from oracle.feature.utils import sequence_summary

_null_feature_gate = {
    "container_tasks_state",
    "container_start_time_seconds",
    "container_scrape_error",
    "container_last_seen",
}


def gen_features(entries: dict, id_fetch: Callable, post_proc: Callable = lambda x: x, post_proc_args=None) -> Dict:
    """Given a list of log entries, return the feature vector. Each entry contains the metadata
    about the metric and its timeseries. The resulting feature vector is indexed by
    the pod name and then the container name.

    If the counter is an accumulator, return its last value, else return a summary for the timeseries."""

    feature_vec = dict()

    print("container feature: total entries", len(entries))
    for e in entries:
        metric = e["metric"]
        values = e["values"]

        name: str = metric["__name__"]
        if name in _null_feature_gate:
            continue

        id_ = id_fetch(metric)
        # an empty id is considered as the feature should be ignored
        if id_ == "":
            continue

        if "total" in name:
            feature_vec["_".join(["cf", id_, name])] = float(values[-1][1])
        else:
            tss = sequence_summary(values)
            for k, v in tss.items():
                feature_vec["_".join(["cf", id_, name, k])] = float(v)

    print("container feature: feature vector", len(feature_vec))
    return post_proc(feature_vec)


if __name__ == '__main__':
    from .gen_test import test
    test()
