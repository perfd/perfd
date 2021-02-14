import json
import pprint as pp
from oracle.feature.dataset_gen import gen_samples
import oracle.feature.spark.gen as spk_gen

from .gen import gen_features


def test():
    s = gen_samples(spk_gen.get_spark_log_index(), num_sample=1)
    _, _, _, out_entries = s[0]
    fv = gen_features(json.loads(out_entries["node"]))
    pp.pprint(fv)
    print("total # of features:", len(fv))
    print("total # of non-zero features:", sum([1 for f, v in fv.items() if v != 0]))


if __name__ == '__main__':
    # test()
    from kubernetes import config
    from kubernetes.client.apis import core_v1_api

    import uuid
    from typing import Iterable, Set, List, Dict
    import numpy as np
    import pprint as pp

    # initial setup for kube-client TODO: just delete the commented??
    def init_k8s_client():
        config.load_kube_config()
        api = core_v1_api.CoreV1Api()
        return api
    
    api = init_k8s_client()
    pp.pprint(api.list_node())
