import ray
import os
import sys
import random

path_join = os.path.join
real_path = os.path.realpath

perfd_dir = real_path(path_join(os.getcwd()))
microps_dir = path_join(perfd_dir, "thirdparty", "microps")
sys.path += [perfd_dir, microps_dir]

from thirdparty.microps.oracle.experiments.spark_sql_perf.main import SparkExperiment, SparkBenchMaker
from thirdparty.microps.build.spark.driver import add_role as add_spk_role
import thirdparty.microps.oracle.apps.spark_sql_perf.configs as spk
import thirdparty.microps.oracle.experiments.spark_sql_perf.utils as utils


@ray.remote
def run(run_config: dict, wrks: dict) -> dict:
    try:
        add_spk_role()
    except:
        print("run, spark: ignore")
    os.chdir(microps_dir)

    # TODO: add virtual cluster labels to the pods
    base_spk_config = spk.apps_config_map["sparkperfml"]

    # TODO: update driver and executor memory
    base_spk_config = spk.patched_app_config(base_spk_config,
                                             {
                                                 "app_name": run_config["appName"],
                                                 "ins_type": run_config["serverInstanceType"],
                                                 "ins_num": run_config["numExecutor"] + 1,
                                                 # "node_selectors": cur_node_selectors,
                                                 "driver_adaptive_gc": run_config["driverAdaptiveGC"],
                                             })

    bench = None
    for b in SparkBenchMaker.load_benchmarks():
        if b["name"] == run_config["appName"]:
            bench = b
    if bench is None:
        print("run, spark: unable to find bench", run_config["appName"])

    # spark sql perf configurations
    config_base = SparkBenchMaker.load_base()
    # change the dataset scale
    utils.update_bench_params(base=config_base, bench=bench,
                              key="numExamples", value=run_config["inputScale"], is_scale=True)

    # change number of partition, each executor has at least one partition
    utils.update_bench_params(base=config_base, bench=bench,
                              key="numPartitions", value=run_config["numPartition"], is_scale=False)
    utils.update_bench_params(base=config_base, bench=bench,
                              key="randomSeed",
                              value=random.randint(0, 10000) if run_config.get("randomSeed", 1) == "random" else 1,
                              is_scale=False)

    bc = SparkBenchMaker.patched_bench_config(config_base,
                                              {
                                                  "benchmarks": [bench]
                                              })

    print(bc)
    exp = SparkExperiment(
        {
            "app_configs": base_spk_config,
            "exp_configs": {
                "s3_log_bucket": run_config["logBucket"],
                "num_executor": run_config["numExecutor"],
                "ins_type": run_config["serverInstanceType"],
                "ins_num": run_config["numServerInstance"],
                "run_interval": 0.5,
                "runs": 1,
                "bench_config": bc,
            },
            "ins_type_num": [(run_config["serverInstanceType"], run_config["numServerInstance"])],
            "variables": {},
        }
    )
    exp.run()
    return {}
