import json
from typing import Dict
from collections import defaultdict

import oracle.configs as oracle_cg
from oracle.utils import timed, s3
from oracle.feature.utils import sequence_summary

static = {
    "num_executor",
    "batch_size",
    "num_partition",
    "jct",  # note that jct is in every feature gate
}

dynamic = {
    "internal.metrics.executorCpuTime",
    "internal.metrics.executorDeserializeCpuTime",
    "internal.metrics.executorDeserializeTime",
    "internal.metrics.executorRunTime",
    "internal.metrics.jvmGCTime",
    "internal.metrics.resultSerializationTime",
    "internal.metrics.resultSize",
    "internal_metrics_input_bytesRead",
    "internal_metrics_input_recordsRead",
    "jct",
}


@timed
def get_spark_log_index(prefix: str="sparkperf", log_bucket=oracle_cg.s3_log_bucket) -> dict:
    print("fetching the log index..")
    return create_log_entry_index(get_log_summaries(prefix=prefix, log_bucket=log_bucket), log_bucket=log_bucket)


def get_log_summaries(prefix: str = "sparkperf", log_bucket=oracle_cg.s3_log_bucket) -> list:
    bucket = s3.s3_res.Bucket(log_bucket)
    objs = [i for i in bucket.objects.filter(Prefix=prefix)]
    return objs


def create_log_entry_index(log_objs: list, log_bucket=oracle_cg.s3_log_bucket) -> dict:
    """Returns lookup tables for locating log entries.

    Given an application's name/id, the log index returns the entry index to the corresponding logs entries.

    Treating the S3 object key as a file path, we use first path segment as the log_id (the key)
    and the last path_segment as an log entry's name.

    A log consists of input and output entries, e.g.,

    - bucket/log-root
        - /input/config.json
        - /output/eventlogs/xxx
        - /output/container/xxx
        - /output/node/xxx

    To get an entry: app_id -> log_id -> entry_index.
    """
    log_index = defaultdict(lambda: defaultdict(list))
    for obj in log_objs:
        segs = obj.key.split("/")
        log_id = segs[0]
        app_id = "-".join(log_id.split("-")[:2])
        log_entry = LogEntry(bucket=log_bucket, key=obj.key, name=segs[-1])
        # print("segments:", segs)

        if "output" in segs:
            log_entry.entry_type = "output"
            if "eventlogs" in segs:
                log_entry.name = "eventlogs"
            elif "container" in segs:
                log_entry.name = "container"
            elif "node" in segs:
                log_entry.name = "node"
            elif "bench" in segs:
                log_entry.name = "bench"
            elif "driver" in segs:
                log_entry.name = "driver"
            else:
                continue
            log_index[app_id][log_id].append(log_entry)

        elif "input" in segs:
            log_entry.entry_type = "input"
            if "bench_configs" in segs[-1]:
                log_entry.name = "bench_configs"
            elif "configs" in segs[-1]:
                log_entry.name = "app_configs"
            log_index[app_id][log_id].append(log_entry)

        elif "info" in segs:
            log_entry.entry_type = "info"
            log_index[app_id][log_id].append(log_entry)

        elif len(segs) >= 2 and segs[1] == "_SUCCESS":
            log_entry.entry_type = "success"
            log_index[app_id][log_id].append(log_entry)

        else:
            pass

    return log_index


class LogEntry:
    def __init__(self, bucket: str = "", key: str = "", name: str = "", entry_type: str = ""):
        self.bucket = bucket
        self.key = key
        # the name of the log entry, e.g., an S3 object name
        self.name = name
        # input or output
        self.entry_type = entry_type

    def __repr__(self):
        return json.dumps(self.__dict__, indent=5)


def gc_logs(quiet: bool = True, prefix: str = "spark"):
    for b in s3.list_buckets():
        if b.startswith("microps-logs"):
            bucket = s3.s3_res.Bucket(b)
            for o in bucket.objects.all():
                if o.key.startswith(prefix):
                    o.delete()
    if not quiet:
        print("Done.")


def spark_container_feature_id(metadata: dict) -> str:
    if metadata["container_name"] == "":
        return ""
    else:
        pod_id = "driver" if "driver" in metadata["pod_name"] \
            else "_".join(metadata["pod_name"].split("-")[-2:])
        return pod_id + "_" + metadata["container_name"]


def gen_post_proc_func(select_feature=None, select_percentile=0.95):
    def post_proc(fv) -> Dict:
        """Post processing feature vector.

        If a select_feature is given, the function pick the executor and all its
        containers by selecting the value on the feature at the given percentile.

        Otherwise, the function generates a summary of the distribution of all
        executors."""

        driver_fv = dict()
        executor_fv = defaultdict(lambda: defaultdict(float))
        prefix, selected_executor_id = "cf_exec", None

        for k, v in fv.items():
            if "driver" in k:
                driver_fv[k] = v
            else:
                executor_id = k.split("_")[2]
                feature_id = "_".join(k.split("_")[3:])  # container_id + feature_name
                executor_fv[feature_id][executor_id] = v

        if select_feature is not None:
            pairs = list(executor_fv[select_feature].items())
            pairs = sorted(pairs, key=lambda x: x[1])

            idx = int(len(pairs) * select_percentile)
            idx = len(pairs) - 1 if idx >= len(pairs) else idx

            selected_executor_id = pairs[idx][0]

        for f, execs in executor_fv.items():
            if selected_executor_id is not None:
                driver_fv[prefix + "_" + f] = execs[selected_executor_id]
            else:
                # generate the percentile summary
                for k, v in sequence_summary(execs.items()).items():
                    driver_fv[prefix + "_" + f + "_" + k] = float(v)
        return driver_fv

    return post_proc


if __name__ == '__main__':
    from oracle.feature.spark import test_spark

    print(spark_container_feature_id(test_spark.metadata))
