from kubernetes import config
from kubernetes.client.apis import core_v1_api

import uuid
from typing import Iterable, Set, List, Dict
import numpy as np

import oracle.feature.cloud.gen_aws_ec2 as aws


# initial setup for kube-client TODO: just delete the commented??
# def init_k8s_client():
#     config.load_kube_config()
#     api = core_v1_api.CoreV1Api()
#     return api
#
#
# def launch_app(name,
#                tag=str(uuid.uuid4()),
#                image="nginx",
#                duration=10,
#                scheduler="default-scheduler",
#                api=None):
#     if api is None:
#         api = init_k8s_client()
#     resp = None
#
#     if not resp:
#         print("Pod %s does not exits. Creating it..." % name)
#         # check whether the name has tag if not, at the latest tag to it
#         if ":" not in image:
#             image += ":latest"
#         pod_manifest = {
#             'apiVersion': 'v1',
#             'kind': 'Pod',
#             'metadata': {
#                 'name': name
#             },
#             'spec': {
#                 "schedulerName": scheduler,
#                 "activeDeadlineSeconds": 320,
#                 "restartPolicy": "Never",
#                 'containers': [{
#                     'image': image,
#                     'name': 'sleep',
#                     "command": ["/bin/sh"],
#                     "args": [
#                         "-c",
#                         "sleep {}".format(duration)
#                     ]
#                 }]
#             }
#         }
#         resp = api.create_namespaced_pod(body=pod_manifest,
#                                          namespace='default',
#                                          )
#         print("Done.")
#         return tag


def normalized_feature_name(ft: str) -> str:
    return ft.replace(".", "_").replace(":", "_").replace(" ", "_")


def normalized_feature_names(fts: Iterable) -> Set:
    return set([normalized_feature_name(f) for f in fts])


def sequence_summary(ts: Iterable) -> Dict:
    """Given a sequence of data points, perform 'max_pooling'-like operation with
    a predefined set of statistical metrics."""

    ts = [float(v) for _, v in ts]

    return {
        "max": np.max(ts),
        "mean": np.mean(ts),
        "min": np.min(ts),
        "5_pc": np.percentile(ts, 5),
        "25_pc": np.percentile(ts, 25),
        "50_pc": np.percentile(ts, 50),
        "75_pc": np.percentile(ts, 75),
        "95_pc": np.percentile(ts, 95),
    }


def make_feature_mod(mode="all", feature_gate=None, feature_select=None, quiet=False):
    """The feature_mod func should do three things:
        - drop jct
        - allow gated features to passthrough
        - select features
    """
    feature_gate = {} if feature_gate is None else feature_gate
    feature_select = {} if feature_select is None else feature_select

    if type(mode) == list or type(mode) == set:
        mode = set(mode)

    def feature_mod(features):
        # TODO: move the for loop inside mode condition and use lambda to pass condition
        new_features = list()

        for f in features:
            if f == "jct" or "jct" in f:
                continue

            if f in feature_gate or f in feature_select:
                new_features.append(f)
                continue

            if type(mode) == set:
                if f in mode:
                    new_features.append(f)
                continue

            assert type(mode) == str
            if mode == "all":
                new_features.append(f)
            elif mode == "static":
                # if not f.startswith("cf") and not f.startswith("nf") \
                #         and not f.startswith("internal") and "jct" not in f:
                if f in aws.static:
                    new_features.append(f)
            elif mode == "ins":
                if f == "API_Name":
                    new_features.append(f)
            elif mode == "none":
                continue
            elif mode == "cf":
                if f.startswith("cf"):
                    new_features.append(f)
            elif mode == "gcf":
                if f.startswith("gcf_driver_full_gc_count"):
                    new_features.append(f)
            elif mode == "gcf_ne":
                if f.startswith("gcf_driver_full_gc_count") or f.startswith("num_executor"):
                    new_features.append(f)
            elif mode == "gcf-all":
                if f.startswith("gcf"):
                    new_features.append(f)
            elif mode == "non_cf":
                if not f.startswith("cf"):
                    new_features.append(f)
            elif mode == "cf_total":
                if f.startswith("cf") and "total" in f:
                    new_features.append(f)
            elif mode == "fw":
                if f.startswith("internal"):
                    new_features.append(f)
            elif mode == "cf_fw":
                if f.startswith("internal") or f.startswith("cf"):
                    new_features.append(f)
            elif mode == "cf_total_fw":
                if f.startswith("internal") or (f.startswith("cf") and "total" in f):
                    new_features.append(f)
            elif mode == "cpu_only":
                if "container_cpu_usage_seconds_total" in f:
                    new_features.append(f)
            elif mode == "node_all":
                if f.startswith("nf"):
                    new_features.append(f)
            elif mode == "node_all_ne":
                if f.startswith("nf") or f.startswith("num_executor"):
                    new_features.append(f)
            elif mode == "node_free":
                if f.startswith("nf") and ("avail" in f or "free" in f or "Free" in f or "Avail" in f):
                    new_features.append(f)
            elif mode == "node_not_total":
                if f.startswith("nf") and "total" not in f:
                    new_features.append(f)
            else:
                raise Exception("Feature mod: unsupported mode: " + str(mode))

        if not quiet:
            print("reduce {} features".format(len(features) - len(new_features)))
        return new_features

    return feature_mod


make_feature_mode = make_feature_mod
