import os
import sys
import pandas as pd
import yaml

from apps.net.util import s3

path_join = os.path.join
real_path = os.path.realpath

perfd_dir = real_path(path_join(os.getcwd(), "..", ".."))
generic_dir = path_join(perfd_dir, "apps", "generic")
microps_dir = path_join(perfd_dir, "thirdparty", "microps")
sys.path += [perfd_dir, microps_dir]

_root_dir = perfd_dir


def load(config_file=None, bucket=None, app_name=None, trial=None) -> (pd.DataFrame, dict):
    if config_file is not None:
        with open(path_join(_root_dir, config_file), "r") as f:
            spec = yaml.safe_load(f)
            bucket = spec["metadata"]["resultBucket"] if bucket is None else bucket
            app_name = spec["appName"] if app_name is None else app_name
            trial = spec["metadata"]["name"] if trial is None else trial
    else:
        assert not (bucket is None or trial is None or app_name is None)
        spec = {
            "metadata": {
                "name": trial,
                "appName": app_name,
            },
        }
    prefix = s3.path_join(app_name, trial)
    objs = s3.list_objects(bucket, prefix)
    for o in objs:
        if o == s3.path_join(app_name, trial, "df.pickle"):
            return s3.download_object_as(bucket,
                                         o,
                                         lambda x: pd.read_pickle(x).infer_objects()), spec
    print(f"load: unable to find trial: {app_name} {trial}")


def load_given_ctx(ctx: dict):
    return {
        ctx["app_name"]: load(bucket=ctx["exp_name"])
    }, dict()
