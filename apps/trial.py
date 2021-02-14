import os
import sys
import yaml
import time

import ray
import pandas as pd
import pprint as pp
from typing import List
from collections import defaultdict
import hashlib

from apps.util import exit_str, ray as ray_util
from apps.net.util import k8s, s3
import hack.ec2.build.kube.cluster as kops
import hack.ec2.build.kube.autoscaler as kops_scaler

_verbosity = 0
_dir = os.path.dirname(os.path.realpath(__file__))
path_join = os.path.join

"""
Each new application should:
    1. implement the run(run_config: dict, wrks: dict) method;
    2. update the following runner table pointing to the run();
* run_config contains k-v pairs for run configurations;
* wrks contains lists of workers (IPs), each indexed by the instance type;

See apps/memcached/run.py as an example.
"""
from apps.spark.run import run as spk_run
from apps.spark.run_graph import run as spk_run_graph
from apps.memcached.run import run as md_run
from apps.nginx.run import run as nx_run
from apps.influxdb.run import run as ib_run
from apps.microservice.run import run as me_run
from apps.meshping.run import run as mg_run

support_app_runner = {
    "spark": spk_run,  # TODO: make this spark perf run
    "spark_graph": spk_run_graph,  # TODO: make this spark perf run
    "memcached": md_run,
    "nginx": nx_run,
    "influxdb": ib_run,
    "meshping": mg_run,
    "microservice-go-fasthttp": me_run,
    "microservice-node-express": me_run,
    "microservice-akka-http": me_run,
}


def make_trial_config(config_file: str):
    def expand(matrix):
        # validate matrix
        max_col_len = 1
        for v in matrix.values():
            if type(v) == list and len(v) > max_col_len:
                max_col_len = len(v)

        for v in matrix.values():
            if type(v) == list and len(v) != 1 and len(v) != max_col_len:
                exit_str(msg="expand: (i) all rows in trial config must be either of "
                             "length 1 or the same length as the longest list; "
                             "or check if (ii) randomSampling is set")

        ex = dict()
        for k, v in matrix.items():
            if type(v) != list:
                ex[k] = [v] * max_col_len
            elif len(v) == 1:
                ex[k] = v * max_col_len
            else:
                # TODO: mark this as varying feature
                # TODO: randomize the order
                ex[k] = v
        return ex, max_col_len

    def explode(matrix, prefix=""):
        col_sizes = {len(v) for k, v in matrix.items()}
        assert len(col_sizes) == 1, "explode: columns have different sizes"
        num_row = list(col_sizes)[0]

        _rows, ctr = list(), 0
        while ctr < num_row:
            _rows.append({prefix + k: v[ctr] for k, v in matrix.items()})
            ctr += 1
        return _rows

    def sample_combination(pool: dict, num_sample=200):
        import itertools
        import random

        kls = list()
        for k, v in pool.items():
            kl = list()
            if type(v) is list:
                for i in v:
                    kl.append((k, i))
            else:
                kl.append((k, v))
            kls.append(kl)
        flattened_pool = list(itertools.product(*kls))

        _rows = list()
        for _f in random.sample(flattened_pool, num_sample):
            _rows.append({_k: v for _k, v in _f})
        return _rows

    class TrialConfig:
        # TODO: a robust schema for TrialConfig
        def __init__(self):
            self.trial_id = None

            self.meta_data = None
            self.meta_config = None
            self.resource_config = None
            self.app_config = None
            self.all_config = None

            self.num_run = 0
            self.num_trial = 0

            self._runs = None
            self._igs = None

        @property
        def runs(self):
            # turns a trial config to a list of run configs.
            def get_ins_type_num(c):
                itn, aux_num, aux_type = defaultdict(int), dict(), dict()
                available_ins = {ig["instanceType"] for ig in self.ins_groups}

                for k, v in c.items():
                    if k not in self.resource_config:
                        continue
                    if k.startswith("num"):
                        name = k.replace("num", "")
                        aux_num[name.lower()] = v
                    elif k.endswith("Type"):
                        name = k.replace("Type", "")
                        assert v in available_ins, "trial: config error, " \
                                                   "missing instance type"
                        aux_type[name.lower()] = v
                for n, num in aux_num.items():
                    assert n in aux_type, "trial: config error, " \
                                          "inconsistent resourceConfig"
                    itn[aux_type[n]] += num
                return itn

            if self._runs is None:
                runs = list()
                num_sample = self.meta_data.get("randomConfigSampling", None)

                if num_sample is None:
                    rcs = explode(self.all_config)
                else:
                    rcs = sample_combination(self.all_config, num_sample)

                for rc in rcs:
                    igs = get_ins_type_num(rc)
                    feature_digest = hashlib.md5(str(rc).encode()).hexdigest()
                    for i in range(int(rc["numRun"])):
                        _rc = dict(rc)
                        # extra run configs to be add below
                        _rc["feature_digest"] = feature_digest
                        _rc["logBucket"] = self.meta_data["logBucket"]
                        _rc["debug"] = self.meta_data.get("debug", False)
                        # TODO: fix run id, identify the varying feature
                        _rc["run_id"] = s3.path_join(self.trial_id, str(i))
                        _rc["ins_type_num"] = igs
                        runs.append(_rc)
                self._runs = runs
            return self._runs

        @property
        def ins_groups(self):
            if self._igs is None:
                image, tenancy, igs = "", "", list()
                for ig in tc.meta_data["instanceGroup"]:
                    image = ig.get("image", image)
                    tenancy = ig.get("tenancy", tenancy)
                    assert image != "" and tenancy != "", \
                        "trial: missing tenancy or image in config"

                    ig.update({
                        "image": image,
                        "tenancy": tenancy,
                    })
                    igs.append(ig)
                self._igs = igs
                self.meta_data["tenancy"] = tenancy
            return self._igs

        def validate(self):
            # required fields
            for k in {"tenancy", "logBucket", "resultBucket"}:
                assert k in self.meta_data
            for k in {"numRun"}:
                assert k in self.all_config
            print("trial: config validated.")
            # TODO: add more validation

    tc = TrialConfig()

    # parse configs
    with open(path_join(_dir, config_file), "r") as f:
        raw = yaml.safe_load(f)
        tc.meta_data = raw["metadata"]
        tc.meta_data["instanceGroup"] = raw["spec"]["instanceGroup"]

        if tc.meta_data.get("randomConfigSampling", None):
            preproc = lambda x: (x, -1)
        else:
            preproc = expand

        # TODO: add shuffle option when preproc is expand
        tc.meta_config, _ = preproc(raw["spec"]["metaConfig"])
        tc.resource_config, _ = preproc(raw["spec"]["resourceConfig"])
        tc.app_config, _ = preproc(raw["spec"]["appConfig"])
        tc.all_config, tc.num_trial = preproc({
            **raw["spec"]["metaConfig"],
            **raw["spec"]["resourceConfig"],
            **raw["spec"]["appConfig"],
        })
        tc.trial_id = s3.path_join(tc.meta_data["appName"],
                                   tc.meta_data["name"],
                                   s3.timestamp())
        _ = tc.runs
        _ = tc.ins_groups
        tc.num_run = len(tc.runs)
        tc.validate()
    return tc


def sched(tasks: list, runner):
    # assign default tags on the task configs
    for t in tasks:
        t["nodes"] = None
        t["assigned"] = False

    if _verbosity > 1:
        pp.pprint(tasks)

    worker_name_to_ip = k8s.get_worker_external_ip_map()
    np = k8s.NodePool()
    np.discover()

    future_to_task = dict()

    def select(fin_tasks):
        # TODO: add virtual cluster labels
        # free the nodes used by finished tasks
        for fid in fin_tasks:
            task = future_to_task[fid]
            np.free(task["nodes"])

        new_running = list()
        # decide next tasks to run; linear scan
        for task in tasks:
            if np.peek() == 0:
                break
            if task["assigned"]:
                continue

            workers, ins_type_worker = list(), defaultdict(list)
            for ins_type, num in task["ins_type_num"].items():
                if np.peek() == 0:
                    np.free(workers)
                    workers = list()
                    break

                success, picked = np.take(count=num, selectors={
                    "beta.kubernetes.io/instance-type": ins_type,
                })

                if not success:
                    np.free(workers)
                    workers = list()
                    break
                else:
                    workers.extend(picked)
                    ins_type_worker[ins_type].extend([worker_name_to_ip[p]
                                                      for p in picked])

            # succeeds in getting desired workers
            if len(workers) > 0:
                task["assigned"] = True
                task["nodes"] = workers

                future = runner.remote(task, ins_type_worker)
                future_to_task[future] = task
                new_running.append(future)

                print("sched: succeeds, taking %s nodes" % len(workers))

        if len(new_running) == 0:
            print("sched: unable to find or no new tasks to run")
        print("sched:", new_running)
        return new_running

    # slide_wait emulating a task scheduler with worker queueing and idle notification
    ready, _ = ray_util.select_wait(len(tasks),
                                    num_returns=len(tasks),
                                    timeout=None,
                                    select=select)
    return ray.get(ready)


def trial(config_file):
    # TODO: Trial warm starting (e.g., if fail at run)
    # TODO: Mark on S3 for trial completion
    # TODO: add clusterdown option in place of autoscale option
    # TODO: infer best instance numbers to provision
    # TODO: allow multiple k8s clusters on the same
    #  access machine, e.g., via kube context or pass in
    #  different k8s configuration files
    # TODO: checkpoint incremental run results
    # TODO: make placemegroup part of the configuration
    print("trial: start with config file", config_file)

    # load trial specs
    tc = make_trial_config(config_file)

    _app_name = tc.meta_data["appName"]
    if _app_name not in support_app_runner:
        runner = None
        exit_str("unsupported application %s, missing runner" % _app_name)
    else:
        runner = support_app_runner[_app_name]

    global _verbosity
    _verbosity = tc.meta_data.get("verbosity", 0)
    if _verbosity > 0:
        print("trial: %d trials and %d runs" % (tc.num_trial, tc.num_run))
    if _verbosity > 1:
        print("trial: overview of runs and resources")
        pp.pprint(tc.runs)
        pp.pprint(tc.ins_groups)

    ray.init(log_to_driver=True)

    # prepare cluster
    if kops.is_up():
        print("trial: k8s running; check if instance groups match..")
        if tc.meta_data.get("autoscale", True):
            kops_scaler.autoscale(tc.ins_groups)

        print("trial: k8s clearing up..")
        k8s.delete_all_pods()
        k8s.clean_all_vc()
    else:
        print("trial: k8s not ready; starting one..")
        kops.create_and_start(tc.ins_groups)

    def create_if_not_exist(bucket, empty=False):
        if not s3.bucket_exist(bucket):
            s3.create_bucket(bucket)
        elif empty:
            print(f"trial: empty bucket {bucket}")
            s3.empty_bucket(bucket)

    # TODO: fix the logs (currently of no use except for spark)
    create_if_not_exist(tc.meta_data["logBucket"], empty=True)
    create_if_not_exist(tc.meta_data["resultBucket"])

    # start runs
    start = time.time()
    results = sched(tc.runs, runner)
    print(f"trial: runs complete in {time.time() - start}s.")

    # post proc
    exp_path = s3.path_join(tc.meta_data["appName"],
                            tc.meta_data["name"])
    df = postproc(results, tc)

    if tc.meta_data.get("debug", False):
        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None):
            print("trial debug:\n", df)

    # TODO: handle spark experiments result parsing
    # TODO: replace the ad-hoc solution below
    if tc.meta_data["appName"] == "spark":
        if tc.meta_data.get("sparkBench", "spark-sql-perf"):
            from thirdparty.microps.oracle.feature.featuredb import remove_training_data
            from thirdparty.microps.examples.spark_sql_perf.dataset_gen import gen
            from thirdparty.microps.examples.spark_sql_perf.cmd import load_app_dfs

            app_name = "sparkperf-" + tc.app_config["appName"][0]
            bucket_name = tc.meta_data["logBucket"]

            print("debug:", bucket_name)
            remove_training_data(db_name=bucket_name, app_id=app_name)
            gen(bucket_name)
            app_df, _ = load_app_dfs(bucket_name)

            df = app_df[app_name]
            print(df)

    # upload results
    s3.dump_and_upload_file(df, bucket=tc.meta_data["resultBucket"],
                            key=s3.path_join(exp_path, "df.pickle"))
    s3.dump_and_upload_file("",
                            bucket=tc.meta_data["logBucket"],
                            key=s3.path_join(tc.trial_id, "_SUCCEED"))
    print("trial: results uploaded.")

    if tc.meta_data.get("clusterdown", False):
        kops.delete_cluster()
        print("trial: cluster down.")


def postproc(results: List[dict], trial_config) -> pd.DataFrame:
    df = pd.DataFrame(results)
    df["tenancy"] = trial_config.meta_data["tenancy"]

    try:
        df = df.drop(columns=["ins_type_num",
                              "logBucket",
                              "run_id",
                              "nodes",
                              "assigned",
                              "numRun"]).reset_index()
    except:
        pass

    if not trial_config.meta_data.get("debug", False):
        to_drop = list()
        for c in df.columns:
            if c.startswith("debug_"):
                to_drop.append(c)
        df = df.drop(columns=to_drop).reset_index()
    return df


def main():
    if len(sys.argv) > 1:
        trial(sys.argv[1])
    else:
        exit_str(msg="run: please specify config file")


if __name__ == '__main__':
    main()
