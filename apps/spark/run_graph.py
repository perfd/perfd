import ray
import os
import sys
import random

path_join = os.path.join
real_path = os.path.realpath

perfd_dir = real_path(path_join(os.getcwd()))
microps_dir = path_join(perfd_dir, "thirdparty", "microps")
sys.path += [perfd_dir, microps_dir]

from thirdparty.microps.oracle.experiments.spark_graph.main import SparkExperiment
from thirdparty.microps.build.spark.driver import add_role as add_spk_role
import thirdparty.microps.oracle.apps.spark_graph.configs as spk


@ray.remote
def run(run_config: dict, wrks: dict) -> dict:
    try:
        add_spk_role()
    except:
        print("run, spark: ignore")
    os.chdir(microps_dir)

    # TODO: add virtual cluster labels to the pods
    base_spk_config = spk.apps_config_map["sparkpagerank"]

    ds = int(spk.DEFAULT_DATASET_SIZE * 0.01 * run_config["inputScale"])
    # TODO: update driver and executor memory
    base_spk_config = spk.patched_app_config(base_spk_config,
                                             {
                                                 "app_name": run_config["appName"],
                                                 "ins_type": run_config["serverInstanceType"],
                                                 "ins_num": run_config["numServerInstance"],
                                                 # "node_selectors": cur_node_selectors,
                                                 "driver_adaptive_gc": run_config["driverAdaptiveGC"],
                                                 "args": "-seed={} ".format(-1) +
                                                         "-niters=10 " +
                                                         "-nverts={}".format(ds),
                                                 "configs_str":
                                                     "--conf spark.scheduler.minRegisteredResourcesRatio=1 " +
                                                     "--conf spark.locality.wait=0s ",
                                                 "dataset_size": run_config["inputScale"],
                                             })

    exp = SparkExperiment(
        {
            "app_configs": base_spk_config,
            "exp_configs": {
                "s3_log_bucket": run_config["logBucket"],
                "num_executor": run_config["numServerInstance"] - 1,
                "ins_type": run_config["serverInstanceType"],
                "ins_num": run_config["numServerInstance"],
                "run_interval": 0.5,
                "runs": 1,
            },
            "ins_type_num": [(run_config["serverInstanceType"], run_config["numServerInstance"])],
            "variables": {},
        }
    )
    exp.run()
    return {}
