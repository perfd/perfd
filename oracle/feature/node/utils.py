from kubernetes import config
from kubernetes.client.apis import core_v1_api


k8s_client = None


def init_k8s_client():
    config.load_kube_config()
    api = core_v1_api.CoreV1Api()
    return api


def get_node_raw(client=None):
    """
    TODO: check if this may generate dirty data with missing node
    TODO: select subset of nodes instead of all
    """
    global k8s_client

    if client is None:
        if k8s_client is None:
            k8s_client = init_k8s_client()
        client = k8s_client

    return client.list_node().items


def gen_node_info_by_name(node_info):
    results = {}
    for n, i in node_info.items():
        i["ip"] = n
        results[i["name"]] = i
    return results


def parse_and_return_node_config_map(nodes):
    """Return a map of node's IP address to its metadata (role, labels etc.)"""
    node_map = dict()

    for n in nodes:
        ip = parse_and_return_node_ip(n)
        node_map[ip] = {
            "role": n.metadata.labels["kubernetes.io/role"],
            "labels": n.metadata.labels,
            "name": n.metadata.name,
            "namespace": n.metadata.namespace,
        }

    return node_map


def parse_and_return_node_ip(n, ip_type="InternalIP"):
    addrs = n.status.addresses
    for a in addrs:
        if a.type == ip_type:
            return a.address


if __name__ == '__main__':
    import pprint as pp
    import json
    pp.pprint(json.dumps(parse_and_return_node_config_map(get_node_raw())))
