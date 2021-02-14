import json
import yaml
import time
from collections import defaultdict

from pyspark.sql import SparkSession, DataFrame

import oracle.feature.cloud.gen_aws_ec2 as ec2_feature
import oracle.feature.container.gen as con_gen
import oracle.feature.container.utils as con_utils
import oracle.feature.node.gen as node_gen
import oracle.utils as utils
import oracle.feature.spark_sql_perf.gen as spk_gen

"""TODO: Move this to oracle.feature.spark"""


class Parser:
    """Stateful log parsing."""

    def __init__(self, log_raw: str = None):
        self.log = log_raw


class RuntimeParser(Parser):
    """Parses runtime logs."""

    def parse(self, quiet: bool = True):
        if not quiet:
            print(self.log)


def get_json(line):
    # return json.loads(line.strip("\n").replace("\n", "\\n"))
    return json.loads(line)


class DFParser(Parser):
    """Parses using Spark DataFrame."""

    def __init__(self, log_raw: str = None, spark_session: SparkSession = None):
        # DFParser will create its own spark session if not provided to run standalone
        if spark_session:
            self.spark_session = spark_session
            self.stand_alone = False
        else:
            self.stand_alone = True
        super(DFParser, self).__init__(log_raw)

    def __getattr__(self, item):
        if item == "spark_session":
            # TODO: better memory limit configurations here
            self.spark_session: SparkSession = SparkSession.builder \
                .master("local") \
                .appName("microps") \
                .config("spark.driver.memory", str(utils.get_sys_memory() * 1 / 4)) \
                .getOrCreate()
            return self.spark_session

    def parse(self, quiet: bool = True):
        if not quiet:
            return self.spark_session

    def parse_log(self, info, input_entries, output_entries, is_eventlog=True, is_node=True, is_container=True) -> dict:
        spark = self.spark_session
        sc = spark.sparkContext

        # get the metadata about the experiment
        metadata = info

        # pre-process and merge inputs and outputs; merged dict should include the following highest
        # level keys: "app", "resource", and "output"
        start = time.time()
        merged_dict = {**self.preproc_input(input_entries),
                       **self.preproc_output(output_entries, metadata, is_eventlog, is_node, is_container)}
        print("pre-proc and merging took: %d seconds" % (time.time() - start))

        print("merged dict:", merged_dict["jct"])
        if merged_dict["jct"] < 0:
            print(merged_dict["jct"], merged_dict["resource"]["API_Name"])
            raise Exception

        start = time.time()
        df_merged: DataFrame = spark.read.json(sc.parallelize([json.dumps(merged_dict)]))

        dimensions = ["app.*", "bench.*", "resource.*", "output.*", "jct"]
        df_merged = df_merged.select(*dimensions)
        print("post-proc took: %d seconds" % (time.time() - start))

        return df_merged.collect()[0].asDict()

    def preproc_input(self, entries: dict) -> dict:
        """Returns the dict of flattened inputs."""
        results = dict()

        for name, entry in entries.items():
            # TODO: parse the network parameters if they are there
            if name == "app_configs":
                entry = entry.decode("utf-8")
                entry_dict = json.loads(entry)

                # convert the instance type to resource configuration; currently assumes homogeneous instance type
                ins_type, num_ins = list(entry_dict["resource"].items())[0]

                entry_dict["resource"] = {
                    **ec2_feature.aws_resource_map[ins_type],
                    "num_instance": num_ins,
                }

                # select a subset of application configs
                fields = ["num_executor", "batch_size"]

                if "driver_memory" in entry_dict["app"]:
                    fields.append("driver_memory")
                    m: str = entry_dict["app"]["driver_memory"]
                    entry_dict["app"]["driver_memory"] = int(m.rstrip("m"))

                app = {f: entry_dict["app"][f] for f in fields}

                if "exp_configs" in entry_dict["app"]:
                    if "rate"  in entry_dict["app"]["exp_configs"]:
                        app["rate"] = entry_dict["app"]["exp_configs"]["rate"]
                        app["delay"] = entry_dict["app"]["exp_configs"]["delay"]
                        app["loss"] = entry_dict["app"]["exp_configs"]["loss"]

                results = {
                    **results,
                    # "app": {f: entry_dict["app"][f] for f in fields if f in entry_dict["app"]},
                    "app": app,
                    "resource": entry_dict["resource"],
                }
            elif name == "bench_configs":
                entry = entry.decode("utf-8")
                parsed = yaml.load(entry)

                print("debug:", parsed)
                features = parsed["common"]

                results = {
                    **results,
                    "bench": {
                        "dataset_size": features["numExamples"],
                        # "num_partition": features["numPartitions"]
                    },
                }
        return results

    def preproc_output(self, entries: dict, metadata: dict, is_eventlog=True, is_node=True, is_container=True) -> dict:
        """Returns the dict of flattened outputs."""
        sc = self.spark_session.sparkContext
        sql = self.spark_session.sql

        results = {
            "output": {},
            "jct": 0,
        }

        for name, entry in entries.items():
            # process container logs
            if name == "node":
                if not is_node:
                    continue
                pod_node_map = con_utils.get_pod_node_map(entries["container"])
                results["output"].update(node_gen.gen_features(entries=json.loads(entry),
                                                               metadata={
                                                                   "info": metadata,
                                                                   "pod_node_map": pod_node_map,
                                                               }))

            elif name == "container":
                if not is_container:
                    continue
                select_feature = "executor_container_cpu_usage_seconds_total"
                results["output"].update(con_gen.gen_features(entries=json.loads(entry),
                                                              id_fetch=spk_gen.spark_container_feature_id,
                                                              post_proc=spk_gen.gen_post_proc_func(select_feature)))
            elif name == "driver":
                gcp = GCParser()
                log = entry.decode("utf-8")
                results["output"].update(gcp
                                         .parse(log)
                                         .report("gcf_driver_"))
                driver_log_jct = gcp.extract_jct(log)
                results["driver_log_jct"] = driver_log_jct
                if not is_eventlog:
                    results["jct"] = driver_log_jct

            elif name == "bench":
                results["output"].update({
                    "real_jct": json.loads(entry)["results"][0]["executionTime"],
                })

            # process spark eventlogs
            elif is_eventlog:
                entry = entry.decode("utf-8")
                if len(entry) == 0:
                    continue

                df_root: DataFrame = self.spark_session.read.json(sc.parallelize(entry.split("\n")))

                # df_stage contains the aggregated metrics from the Stage info
                try:
                    df_stage: DataFrame = df_root.filter("Event='SparkListenerStageCompleted'").select("`Stage Info`.*")
                except:
                    print("error:", entry, "entry length:", len(entry))
                    raise Exception

                comp_times = sorted([c[0] for c in df_stage.select("Completion Time").collect()])
                sub_times = sorted([c[0] for c in df_stage.select("Submission Time").collect()])

                jct = comp_times[-2] - comp_times[0]
                full_jct = comp_times[-1] - sub_times[0]

                df_stage.createOrReplaceTempView("t2")
                df_stage = sql("select 'Stage ID', t3.col.* from t2 lateral view explode(Accumulables) t3")
                df_stage.createOrReplaceTempView("t4")
                df_metrics: DataFrame = sql("select Name, sum(Value) as Value from t4 group by Name order by Name")

                for row in df_metrics.collect():
                    results["output"][row.Name] = row.Value
                results["eventlog_jct"] = jct
                results["full_jct"] = full_jct
                results["jct"] = jct
        return results


class SparkSQLPerfBenchParser:
    pass





class GCParser:
    """Parses jvm gc logs with the following options:
    -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps

    Returns a dict of gc related features.
    """
    MINOR_GC = 0
    FULL_GC = 1
    ENTER_ENTRY = 0
    IN_ENTRY = 1
    EXIT_ENTRY = 2

    @staticmethod
    def _extract_region_stats(line: str, young=True) -> (float, float, float):
        sep = "[PSYoungGen: " if young else "[ParOldGen: "
        line = line.split(sep)[1].split()[0].split("->")
        region_before = float(int(line[0].rstrip("K")) / 1000)

        line = line[1].split("(")
        region_after = float(int(line[0].rstrip("K")) / 1000)
        region_total = float(int(line[1].rstrip("K)]")) / 1000)

        return region_before, region_after, region_total

    @staticmethod
    def _extract_whole_stats(line: str):
        line = line.split(", ")[0].split()[-1].split("->")
        before = float(int(line[0].rstrip("K")) / 1000)

        line = line[1].split("(")
        after = float(int(line[0].rstrip("K")) / 1000)
        total = float(int(line[1].rstrip("K)]")) / 1000)
        return before, after, total

    @staticmethod
    def _extract_ada_size_info(line: str):
        if "compute_eden_space_size limits" in line:
            return {
                "avg_young_live": int(line.split("avg_young_live:")[1])
            }
        elif "compute_eden_space_size" in line:
            return {
                "live_space": int(line.split(" live_space: ")[1].split()[0]),
                "free_space": int(line.split(" free_space: ")[1].split()[0]),
                "minor_time": float(line.split(" minor_time: ")[1].split()[0]),
                "major_cost": float(line.split(" major_cost: ")[1].split()[0]),
                "mutator_cost": float(line.split(" mutator_cost: ")[1].split()[0]),
            }
        elif "compute_old_gen_free_space limits" in line:
            return {
                "avg_old_live": int(line.split("avg_old_live:")[1])
            }
        elif "compute_old_gen_free_space" in line:
            return {
                "old_live_space": int(line.split(" live_space: ")[1].split()[0]),
                "old_free_space": int(line.split(" free_space: ")[1].split()[0]),
            }
        elif "tenuring_thresh" in line:
            return {
                "tenure_thresh": int(line.split(" tenuring_thresh: ")[1].split()[0]),
                "avg_survived_padded_avg": float(line.split(" avg_survived_padded_avg: ")[1].split()[0]),
                "avg_promoted_padded_avg": float(line.split(" avg_promoted_padded_avg: ")[1].split()[0]),
                "avg_pretenured_padded_avg": float(line.split(" avg_pretenured_padded_avg: ")[1].split()[0]),
            }
        elif "AdaptiveSizeStart" in line:
            return {
                "ada_col_num": int(line.split(" collection: ")[1].split()[0]),
            }
        elif "survived: " in line:
            return {
                "survived": int(line.split(" survived: ")[1].split()[0]),
                "promoted": int(line.split(" promoted: ")[1].split()[0]),
            }
        else:
            return {}

    @staticmethod
    def _extract_collection(line: str):
        return int(line.split(" collection: ")[1].split()[0])

    @staticmethod
    def print_job_start_end_timestamps(log):
        try:
            FMT = "%Y-%m-%d %H:%M:%S"
            for e in log.split("\n"):
                if "Got job 0" in e:
                    start = " ".join(e.split()[:2])
                elif "Successfully stopped SparkContext" in e:
                    end = " ".join(e.split()[:2])
                elif "Running Spark version" in e:
                    init = " ".join(e.split()[:2])

            tdelta = GCParser._extract_time_diff(init, start, FMT=FMT)
            print("\nInit time:", tdelta)

            tdelta = GCParser._extract_time_diff(start, end, FMT=FMT)
            print("True jct:", tdelta)
        except:
            pass

        # print(log.split("\n")[-10:])

    @staticmethod
    def extract_jct(log):
        for e in log.split("\n"):
            if "Got job 0" in e:
                start = e.split()[1]
            elif "Successfully stopped SparkContext" in e:
                end = e.split()[1]
            elif "Running Spark version" in e:
                init = e.split()[1]

        tdelta = GCParser._extract_time_diff(start, end, as_sec=True)
        return tdelta

    @staticmethod
    def _extract_gc_time(line: str):
        return float(line.split(" secs] [Times:")[0].split()[-1])

    @staticmethod
    def _extract_timestamp(line: str):
        return float(line.split()[1].rstrip(":"))

    @staticmethod
    def _extract_timestamp_date_format(line: str):
        return line.split("T")[1].split(".")[0]

    @staticmethod
    def _extract_timestamp_date_format_2(line: str):
        return line.split()[1]

    @staticmethod
    def _extract_time_diff(start, end, as_sec=False, FMT="%H:%M:%S"):
        from datetime import datetime
        tdelta = datetime.strptime(end, FMT) - datetime.strptime(start, FMT)
        return tdelta.total_seconds() if as_sec else tdelta

    @staticmethod
    def _extract_gc_times(line: str):
        """Returns user, sys, real cpu time in seconds"""
        line = line.split("[Times: ")[1].split()
        u = float(line[0].split("=")[1])
        s = float(line[1].split("=")[1].rstrip(","))
        r = float(line[2].split("=")[1])
        return u, s, r

    def __init__(self):
        self.cur_gc_type = None
        self.cur_entry = None
        self.cur_state = GCParser.EXIT_ENTRY
        self.cur_timestamp = 0
        self.cur_collection = 0

        self.start_timestamp = 0
        self.prev_timestamp = 0
        self.prev_young = 0
        # self.prev_old = 0
        self.prev_total = 0

        # statistics we are interested
        self.minor_gc_cnt, self.full_gc_cnt, self.gc_cnt = 0, 0, 0
        self.minor_gc_total_time, self.full_gc_total_time, self.gc_total_time = 0, 0, 0  # in real time
        self.minor_gc_size, self.full_gc_size = 0, 0

        self.minor_gcs = list()
        self.full_gcs = list()

        self.allocation_rates = list()
        self.allocation_sizes = list()
        self.promotion_rates = list()
        self.promotion_sizes = list()

        self.young_sizes = list()
        self.old_sizes = list()
        self.total_sizes = list()
        self.whole_before, self.whole_after = list(), list()
        self.young_before, self.young_after = list(), list()
        self.old_before, self.old_after = list(), list()
        self.old_free_before, self.old_free_after = list(), list()

        self.ada_info = defaultdict(list)
        self.timestamps = list()
        self.collections = list()

        # spark specific
        self.spark_task_fins = list()
        self.spark_task_ins = list()
        self.pending_task = [(0, 0)]
        self.pending_rdd = [(0, 0)]

    def reset(self):
        self.__init__()
        return self

    def parse(self, log: str, zoom=None, use_col=False):
        for e in log.split("\n"):
            if not self.next_entry(e, use_col):
                continue

            if self.cur_state == GCParser.ENTER_ENTRY:
                self.cur_timestamp = GCParser._extract_timestamp(e)
                self.cur_collection += 1
                if self.cur_collection == 1:
                    self.start_timestamp = GCParser._extract_timestamp_date_format(e)

            if not (zoom is None or (zoom[0] < self.cur_timestamp < zoom[1])):
                continue

            if self.cur_state == GCParser.EXIT_ENTRY:
                # gc durations and counts
                gc_time = self._extract_gc_time(e)
                timestamp = self.cur_timestamp if not use_col else self.cur_collection

                if self.cur_gc_type == GCParser.MINOR_GC:
                    self.minor_gc_cnt += 1
                    self.minor_gc_total_time += gc_time
                    self.minor_gcs.append((timestamp, self.cur_collection))
                elif self.cur_gc_type == GCParser.FULL_GC:
                    self.full_gc_cnt += 1
                    self.full_gc_total_time += gc_time
                    self.full_gcs.append((timestamp, self.cur_collection))

                self.gc_cnt += 1
                self.gc_total_time += gc_time

                # memory sizes and rates
                elapsed_time = self.cur_timestamp - self.prev_timestamp
                self.prev_timestamp = self.cur_timestamp

                young_before, young_after, young_total = GCParser._extract_region_stats(e, young=True)
                whole_before, whole_after, whole_total = GCParser._extract_whole_stats(e)

                self.whole_before.append((timestamp, whole_before))
                self.whole_after.append((timestamp, whole_after))

                self.young_before.append((timestamp, young_before))
                self.young_after.append((timestamp, young_after))
                self.old_before.append((timestamp, whole_before - young_before))
                self.old_after.append((timestamp, whole_after - young_after))

                self.old_free_before.append((timestamp, whole_total - young_total - (whole_before - young_before)))
                self.old_free_after.append((timestamp, whole_total - young_total - (whole_after - young_after)))
                # allocation rate
                alloc = young_before - self.prev_young

                # self.allocation_rates.append((timestamp, alloc / elapsed_time))
                self.allocation_sizes.append((timestamp, alloc))
                self.prev_young = young_after

                # promotion rate; note that it can only be calculated from minor gc log
                if self.cur_gc_type == GCParser.MINOR_GC:
                    promo = (young_before - young_after) - (whole_before - whole_after)
                    # self.promotion_rates.append((timestamp, promo / elapsed_time))
                    self.promotion_sizes.append((timestamp, promo))
                # elif self.cur_gc_type == GCParser.FULL_GC:
                #     old_before, old_after, old_total = GCParser._extract_region_stats(e, young=False)
                #     promo = (young_before - young_after) - (whole_before - whole_after)
                #     self.aapromotion_rates.append((timestamp, promo / elapsed_time))
                #     self.promotion_sizes.append((timestamp, promo))

                # more counters to support detailed analysis
                self.young_sizes.append((timestamp, young_total))
                self.old_sizes.append((timestamp, whole_total - young_total))
                self.total_sizes.append((timestamp, whole_total))

                self.timestamps.append(timestamp)
            elif self.cur_state == GCParser.IN_ENTRY or self.cur_state == GCParser.ENTER_ENTRY:
                info = self._extract_ada_size_info(e)
                for k, v in info.items():
                    self.ada_info[k].append((self.cur_timestamp if not use_col else self.cur_collection, v))
        return self

    def next_entry(self, e, use_col=False) -> bool:
        self.cur_entry = e

        if "[GC (Allocation" in e or "[GC (Metadata" in e:
            self.cur_gc_type = GCParser.MINOR_GC
            self.cur_state = GCParser.ENTER_ENTRY
        elif "[Full GC" in e:
            self.cur_gc_type = GCParser.FULL_GC
            self.cur_state = GCParser.ENTER_ENTRY
        elif "[PSYoungGen" in e:
            self.cur_state = GCParser.EXIT_ENTRY
        # internal transitions
        elif self.cur_state == GCParser.ENTER_ENTRY:
            self.cur_state = GCParser.IN_ENTRY
        elif self.cur_state == GCParser.EXIT_ENTRY:
            # spark specific parsing TODO: split this out, 50 is hardcoded
            def _get_ts():
                if use_col:
                    ts = self.cur_collection
                else:
                    ts = self._extract_time_diff(self.start_timestamp,
                                                 self._extract_timestamp_date_format_2(e),
                                                 as_sec=True)
                    if ts <= self.cur_timestamp:
                        ts = self.cur_timestamp
                return ts

            if "TaskSetManager" in e and "Starting" in e:
                ts = _get_ts()
                self.spark_task_ins.append((ts, (len(self.spark_task_ins) + 1) % 50))
                self.pending_task.append((ts, self.pending_task[-1][1] + 1))
                if "stage 0.0" in e:
                    self.pending_rdd.append((ts, self.pending_rdd[-1][1] + 1))
            elif "TaskSetManager" in e and "Finished" in e:
                ts = _get_ts()
                self.spark_task_fins.append((ts, (len(self.spark_task_fins) + 1) % 50))
                self.pending_task.append((ts, self.pending_task[-1][1] - 1))
            elif "BlockManagerInfo" in e and "Added rdd" in e:
                ts = _get_ts()
                self.pending_rdd.append((ts, self.pending_rdd[-1][1] - 1))
            return False
        return True

    def report(self, prefix="", analyze_mode=False):
        import numpy as np, pprint as pp

        # the analyze mode returns timeseries rather than summarized statistics
        if analyze_mode:
            return {
                **{
                    "allocation_rates": [(i, round(j, 2)) for i, j in self.allocation_rates],
                    "allocation_sizes": [(i, round(j, 2)) for i, j in self.allocation_sizes],
                    "promotion_rates": [(i, round(j, 2)) for i, j in self.promotion_rates],
                    "promotion_sizes": [(i, round(j, 2)) for i, j in self.promotion_sizes],
                    "total_sizes": self.total_sizes,
                    "young_sizes": self.young_sizes,
                    "old_sizes": self.old_sizes,
                    "minor_gcs": self.minor_gcs,
                    "full_gcs": self.full_gcs,
                    "whole_before": self.whole_before,
                    "whole_after": self.whole_after,
                    "young_before": self.young_before,
                    "young_after": self.young_after,
                    "old_before": self.old_before,
                    "old_after": self.old_after,
                    "old_free_before": self.old_free_before,
                    "old_free_after": self.old_free_after,
                    "spark_task_fins": self.spark_task_fins,
                    "spark_task_ins": self.spark_task_ins,
                    "spark_pending_task": self.pending_task,
                    "spark_pending_rdd": self.pending_rdd,
                },
                **self.ada_info}

        return {
            prefix + "minor_gc_count": self.minor_gc_cnt,
            prefix + "minor_gc_time": round(self.minor_gc_total_time, 2),
            prefix + "full_gc_count": self.full_gc_cnt,
            prefix + "full_gc_time": round(self.full_gc_total_time, 2),
            prefix + "gc_count": self.gc_cnt,
            prefix + "gc_time": round(self.gc_total_time, 2),
            prefix + "mem_allocation_rate_mean": round(float(np.mean(np.array([i[1] for i in self.allocation_rates]))),
                                                       2),
            prefix + "mem_promotion_rate_mean": round(float(np.mean(np.array([i[1] for i in self.promotion_rates]))),
                                                      2),
            prefix + "mem_usage": round(float(np.mean(np.array([i[1] for i in self.total_sizes]))), 2),
        }
