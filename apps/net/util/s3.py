import boto3
import os
import json
from collections import OrderedDict
from typing import List
import tempfile
import time
import pandas as pd

import botocore
import botocore.errorfactory
import botocore.client

from . import uuid_str, cmd

s3 = boto3.client('s3')
s3_res = boto3.resource('s3')


def path_join(*args):
    return "/".join(args)


def timestamp():
    return time.strftime("%y.%m.%d..%H.%M.%S")


def create_bucket(bucket_name: str, ACL="private", location="us-west-2"):
    # s3.create_bucket(
    #     Bucket=bucket_name,
    # )
    s3.create_bucket(
        ACL=ACL,
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': location,
        },
    )


def bucket_exist(bucket: str):
    try:
        s3_res.meta.client.head_bucket(Bucket=bucket)
        return True
    except botocore.client.ClientError:
        return False


def empty_bucket(bucket_name: str):
    bucket = s3_res.Bucket(bucket_name)
    bucket.objects.all().delete()


def delete_bucket(bucket_name: str):
    bucket = s3_res.Bucket(bucket_name)
    bucket.objects.all().delete()
    bucket.delete()


def create_dir(bucket_name: str, dir_name: str):
    s3.put_object(
        Bucket=bucket_name,
        Key=(dir_name + "/")
    )


def get_object(bucket: str, prefix: str):
    # Since the key is not known ahead of time, we need to get the
    # object's summary first and then fetch the object
    bucket = s3_res.Bucket(bucket)
    objs = [i for i in bucket.objects.filter(Prefix=prefix)]

    return objs[0].get()["Body"].read()


def download_object_as(bucket, prefix, f=lambda x: pd.read_pickle(x).infer_objects()):
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = tmp_dir + "/{}".format(prefix.split("/")[-1])
        s3.download_file(bucket, prefix, tmp_file)
        return f(tmp_file)


def get_object_last_mod_time(bucket: str, prefix: str):
    bucket = s3_res.Bucket(bucket)
    objs = [i for i in bucket.objects.filter(Prefix=prefix)]

    return objs[0].last_modified


def upload_file(bucket: str, key: str = None, file_: str = None):
    assert not (key is None or file_ is None)
    s3_res.Bucket(bucket).upload_file(file_, key)

    print("s3:", file_, "uploaded, at", bucket + "/" + key)


def dump_and_upload_file(blobs, key: str, bucket, auto_pad=True, raw=False):
    # create a temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        if auto_pad:
            tmp_file = path_join(tmp_dir, key.split("/")[-1] + "-" + uuid_str())
        else:
            tmp_file = path_join(tmp_dir, key.split("/")[-1])
        create_local_file(tmp_file)

        if (type(blobs) in {dict, list, OrderedDict}) and (not raw):
            with open(tmp_file, "r+", encoding='utf-8') as f:
                json.dump(blobs, f)
        elif type(blobs) == pd.DataFrame:
            blobs: pd.DataFrame
            blobs.to_pickle(tmp_file)
        else:
            with open(tmp_file, "r+", encoding='utf-8') as f:
                f.write(str(blobs))

        # TODO: add a lock done file, current version can use the input keys
        upload_file(file_=tmp_file, key=key, bucket=bucket)
        os.remove(tmp_file)


def create_local_file(path):
    basedir = os.path.dirname(path)
    if not os.path.exists(basedir):
        os.makedirs(basedir)
    open(path, "w").close()


def list_buckets() -> List[str]:
    # Call S3 to list current buckets
    response = s3.list_buckets()

    # Get a list of all bucket names from the response
    buckets = [bucket['Name'] for bucket in response['Buckets']]

    # Print out the bucket list
    # print("Bucket List: %s" % buckets)
    return buckets


def list_objects(bucket, key):
    return [c["Key"] for c in s3.list_objects_v2(
        Bucket=bucket,
        Prefix=key)["Contents"]]


def backup_bucket(bucket: str, new_bucket=None):
    new_bucket = new_bucket if new_bucket else bucket + "-backup"
    cmd("aws s3 sync s3://{} s3://{}".format(bucket, new_bucket))
    print("backup of {} at {}".format(bucket, new_bucket))


def get_addr_from_bucket_key(bucket, key):
    return "s3a://" + bucket + "/" + key


"""Log specific operations"""


def get_log_summaries(log_bucket, prefix: str = "spark") -> list:
    bucket = s3_res.Bucket(log_bucket)
    objs = [i for i in bucket.objects.filter(Prefix=prefix)]
    return objs


if __name__ == '__main__':
    print(bucket_exist("microps-bench-spark-sql-perf-dataset-size"))
