import time
import pprint as pp
from collections import defaultdict

from kubernetes import config
from kubernetes import client
from typing import Iterable

import ray

from . import uuid_str, which, cmd

default_excl_selectors = {
    "kubernetes.io/role": "master",
    "kops.k8s.io/instancegroup": "nodes.m4.2xlarge.prom",
}

_config_loaded = False


def create(spec=None):
    pass


def get_service_addr(name):
    # make sure jq exists..
    if which("jq") is None:
        raise Exception("unable to parse service address: jq does not exist")

    return cmd("kubectl get service " + name + " -o json | jq -r '.status.loadBalancer.ingress[0].hostname'"
               , out=True).decode("utf-8").rstrip("\n")


def make_node_affinity_in_spec(selectors):
    return {
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": k,
                                    "operator": "In",
                                    "values": [v],
                                } for k, v in selectors.items()
                            ],
                        }
                    ],
                },
            },
        },
    }


def load_kube_config(reload=False):
    global _config_loaded
    try:
        # per op cost is about 16ms, cache it
        if reload or not _config_loaded:
            print("k8s config reloaded..")
            config.load_kube_config()
            _config_loaded = True
    except Exception as e:
        print("running without k8s running or "
              "exposed in the context: {}.".format(str(e)))


def get_all_pods_name(ns: str = "default") -> Iterable[str]:
    load_kube_config()
    c = client.CoreV1Api()

    pods = c.list_namespaced_pod(namespace=ns).items
    return [pod.metadata.name for pod in pods]


def get_all_pods(ns: str = "default"):
    load_kube_config()
    c = client.CoreV1Api()

    pods = c.list_namespaced_pod(namespace=ns).items
    return pods


def delete_pod(name: str, ns: str = "default"):
    load_kube_config()
    c = client.CoreV1Api()

    body = client.V1DeleteOptions()
    c.delete_namespaced_pod(name, ns)


def delete_svc(name: str, ns: str = "default"):
    cmd("kubectl delete svc {} -n {}".format(name, ns))


def delete_deployment(name: str, ns: str = "default"):
    cmd("kubectl delete deploy {} -n ".format(name) + ns)


def delete_all_pods(ns: str = "default"):
    cmd("kubectl delete pods --all -n " + ns)


def delete_all_deploy(ns: str = "default"):
    cmd("kubectl delete deploy --all -n " + ns)


def delete_all_svc(ns: str = "default"):
    cmd("kubectl delete svc --all -n " + ns)


def delete_all_resource(ns: str = "default"):
    # note that the order matters
    delete_all_svc(ns)
    delete_all_deploy(ns)
    delete_all_pods(ns)


def get_node_pod_map(ns: str = "default", selectors=default_excl_selectors, reverse=True):
    """Pod by node name."""
    pods = get_all_pods(ns)
    nodes = get_all_nodes()

    node_pod_map = {
        n.metadata.labels["kubernetes.io/hostname"]: list()
        for n in nodes if selectors is None or selector_match(n.metadata.labels, selectors, reverse=reverse)
    }

    for p in pods:
        try:
            node_pod_map[p.spec.node_name].append(p)
        except:
            pass
    return node_pod_map


def get_node_pod_count(ns: str = "default", selectors=default_excl_selectors, reverse=True):
    npm = get_node_pod_map(ns, selectors, reverse=reverse)

    return {
        k: len(v) for k, v in npm.items()
    }


def get_all_nodes():
    load_kube_config()
    c = client.CoreV1Api()

    nodes = c.list_node()
    return [n for n in nodes.items]


def get_node_map(selectors=default_excl_selectors, reverse=True, full_object=False):
    """Hostname is used as the the key, the entire label set as the value."""
    nodes = get_all_nodes()
    node_map = dict()

    for n in nodes:
        labels = n.metadata.labels
        if selectors is None or selector_match(labels, selectors, reverse=reverse):
            name = labels["kubernetes.io/hostname"]
            node_map[name] = labels if not full_object else n
    return node_map


def get_worker_external_ip_map() -> dict:
    ips = dict()

    for n, info in get_node_map(full_object=True).items():
        for addr in info.status.addresses:
            if addr.type == "ExternalIP":
                ips[n] = addr.address
    return ips


def get_worker_external_internal_ip_map() -> dict:
    ext_in = dict()

    for n, info in get_node_map(full_object=True).items():
        k, v = None, None
        for addr in info.status.addresses:
            if addr.type == "ExternalIP":
                k = addr.address
            elif addr.type == "InternalIP":
                v = addr.address
        assert not (k is None or v is None)
        ext_in[k] = v
    return ext_in


def selector_match(labels, selectors, reverse=False):
    """If reverse is True, returns False for any matching. This makes exclusive matching easy."""

    for s, v in selectors.items():
        assert s in labels, "non-exist selector: " + s

        if (reverse and labels[s] == v) or (not reverse and labels[s] != v):
            return False
    return True


def get_node_labels(name) -> dict:
    return get_node_map()[name]


def change_node_labels(name, labels: dict):
    """TODO: this should be renamed to update_node_labels or patch node_labels"""
    load_kube_config()
    c = client.CoreV1Api()

    body = {
        "metadata": {
            "labels": labels,
        }
    }
    return c.patch_node(name, body)


@ray.remote
def change_node_labels_async(name, labels: dict):
    return change_node_labels(name, labels)


def change_node_labels_batch(node_labels: dict):
    ray.get([change_node_labels_async.remote(n, ls)
             for n, ls in node_labels.items()])


def remove_node_labels(name, labels):
    load_kube_config()
    c = client.CoreV1Api()

    if type(labels) is dict:
        labels = set(labels.keys())

    body = {
        "metadata": {
            "labels": {
                l: None for l in labels
            },
        }
    }
    return c.patch_node(name, body)


@ray.remote
def remove_node_labels_async(name, labels: dict):
    return remove_node_labels(name, labels)


_virtual_cluster_label = "microps.io/vcid"


class VirtualCluster:
    def __init__(self, nodes):
        self.id = uuid_str()
        self.labels = {
            _virtual_cluster_label: self.id,
        }
        self.nodes = nodes

    def __enter__(self):
        self.apply()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove()

    def apply(self):
        futures = [change_node_labels_async.remote(n, self.labels) for n in self.nodes]
        ray.get(futures)
        return self.id

    def remove(self):
        futures = [remove_node_labels_async.remote(n, self.labels) for n in self.nodes]
        ray.get(futures)
        return self.id

    def validate(self, reverse=False):
        node_set = set(self.nodes)
        for n, ls in get_node_map().items():
            if n not in node_set:
                continue

            if reverse:
                if _virtual_cluster_label in ls and ls[_virtual_cluster_label] == self.id:
                    return False
            else:
                if _virtual_cluster_label not in ls or ls[_virtual_cluster_label] != self.id:
                    return False
        return True


def get_virtual_clusters():
    vc = defaultdict(list)
    for n, labels in get_node_map().items():
        if _virtual_cluster_label in labels:
            vcid = labels[_virtual_cluster_label]
            vc[vcid].append(n)
    return vc


def get_pod_vc_assignment():
    pods = get_all_pods()
    results = dict()

    for p in pods:
        node_selectors = p.spec.node_selector
        if _virtual_cluster_label in node_selectors:
            results[p.metadata.name] = node_selectors[_virtual_cluster_label]
        else:
            results[p.metadata.name] = None
    return results


def get_node_vc(node):
    nl = get_node_map()[node]
    return None if _virtual_cluster_label not in nl else nl[_virtual_cluster_label]


def no_vc_assignment(nodes):
    node_map = get_node_map()

    for n in nodes:
        labels = node_map[n]
        if _virtual_cluster_label in labels and labels[_virtual_cluster_label] is not None:
            return False
    return True


def validate_live_vc():
    vc = get_virtual_clusters()
    pod_vc = get_pod_vc_assignment()

    for p, v in pod_vc.items():
        if v not in vc:
            print("violated:", p, v)
            exit()


def clean_all_vc():
    futures = [remove_node_labels_async.remote(n, {_virtual_cluster_label}) for n in list(get_node_map().keys())]
    ray.get(futures)


def normalized_labels(labels: dict):
    return {
        k.split("/")[1] if "/" in k else k: v for k, v in labels.items()
    }


class Node:
    def __init__(self, labels: dict):
        self.labels = labels
        self.free = True

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.__dict__)


class NodeInfo:
    def __init__(self, addr, labels):
        self.addr = addr
        self.labels = labels

    def __str__(self):
        return str({
            "addr": self.addr,
            "labels": self.labels,
        })

    def __repr__(self):
        return str(self.__dict__)


class NodePool:
    """A local version of the NodePoolService."""

    def __init__(self):
        self.nodes = dict()
        self.num_free = 0

    def __str__(self):
        return str(self.nodes)

    def discover(self, selectors=default_excl_selectors, reverse=True):
        nodes = dict()
        for n, ls in get_node_map(selectors=selectors, reverse=reverse).items():
            nodes[n] = Node(ls)

        self.nodes = nodes
        self.num_free = len(self.nodes)
        return self.nodes

    def take(self, name: str = None, count: int = 1, selectors: dict = None, reverse: bool = False,
             block: bool = False, backoff: bool = True, name_only=True) -> (bool, dict):
        trial_count = 0

        def post_proc(ns):
            if name_only:
                return list(ns.keys())
            else:
                return ns

        while True:
            picked = dict()
            if name is not None and name in self.nodes:
                ni = self.nodes[name]
                if ni.free:
                    ni.free = False
                    picked[name] = ni
            else:
                candidates = dict()
                for name, info in self.nodes.items():
                    if not info.free:
                        continue
                    elif selectors is None or selector_match(info.labels, selectors, reverse=reverse):
                        candidates[name] = info

                if len(candidates) >= count:
                    for name, info in candidates.items():
                        info.free = False
                        self.num_free -= 1
                        picked[name] = info

                        if len(picked) == count:
                            break

            if len(picked) == count:
                return True, post_proc(picked)

            if not block:
                return False, post_proc(picked)

            trial_count += 1

            if backoff:
                time.sleep(1 * trial_count)

    def free(self, names):
        """Free up the node by name. Idempotent."""
        if names is None:
            return

        for n in names:
            self.nodes[n].free = True
            self.num_free += 1

    def peek(self):
        """Estimate of the number of free nodes."""
        return self.num_free


def main():
    import time
    pp.pprint(get_node_pod_count())
    pp.pprint(get_virtual_clusters())
    vc = get_virtual_clusters()
    pod_vc = get_pod_vc_assignment()

    for p, v in pod_vc.items():
        print(v in vc)
    pp.pprint(get_pod_vc_assignment())
    start = time.time()
    load_kube_config()
    print("first op", time.time() - start)


if __name__ == '__main__':
    main()
