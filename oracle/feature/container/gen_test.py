import json
import pprint as pp
from oracle.feature.dataset_gen import gen_samples
import oracle.feature.spark.gen as spk_gen

from .gen import gen_features


def test():
    s = gen_samples(spk_gen.get_spark_log_index(), num_sample=1)
    _, _, _, out_entries = s[0]
    fv = gen_features(json.loads(out_entries["container"]),
                      spk_gen.spark_container_feature_id,
                      spk_gen.gen_post_proc_func(select_feature="executor_container_cpu_usage_seconds_total"))
    pp.pprint(fv)
    print("total # of features:", len(fv))
    print("total # of non-zero features:", sum([1 for f, v in fv.items() if v != 0]))