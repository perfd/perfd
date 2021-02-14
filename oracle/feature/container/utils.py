import json
import pprint as pp


def get_pod_node_map(entries) -> dict:
    """Return the pod_id to node_name mappings."""

    entries = json.loads(entries)
    pod_node_map = dict()

    for e in entries:
        metric = e["metric"]
        values = e["values"]

        pod: str = metric["pod_name"]
        node: str = metric["instance"]

        pod_node_map[pod] = node

    return pod_node_map

