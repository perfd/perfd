import json
import random
import copy
from typing import List, Generator

from pyspark.sql import SparkSession

import ray

import oracle.feature.spark.parser as spk_parser
import oracle.feature.spark_sql_perf.parser as spk_perf_parser
from oracle.feature import featuredb
from oracle.feature.utils import normalized_feature_name
import oracle.configs as oracle_cg
import oracle.utils as utils
from oracle.utils import s3, timed


@timed
def gen_training_data(log_index: dict, batch_size, parser_type):
    """Generate training data and write to the database. Returns false if batch_size is hit.

    TODO: rename training_data to dataset"""
    spark = SparkSession.builder \
        .master("local") \
        .appName("microps") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

    if parser_type == "spark":
        parser = spk_parser.DFParser(spark)
    elif parser_type in {"spark_sql_perf", "spark-sql-perf"}:
        parser = spk_perf_parser.DFParser(spark)
    else:
        raise NotImplementedError

    mongo = featuredb.FeatureDBClient()

    print("generating training data..batch_size: {}".format(batch_size))

    last_app_id, app_rows = None, dict()
    min_feature_set_by_app, min_feature_set = dict(), set()

    # fetch and parse logs
    for app_id, log_id, info, in_entries, out_entries in get_entries(log_index, mongo):
        if last_app_id is None:
            last_app_id = app_id

        if app_id != last_app_id or len(app_rows) >= batch_size:
            mongo.put_training_data(last_app_id, app_rows)
            app_rows, min_feature_set = dict(), set()

        # t = list(in_entries.values())[0].decode("utf-8")
        # pprint.pprint(json.loads(t))

        # out_entries includes container, log, and eventlogs, missing any one will skip this record

        if len(info) and len(in_entries) and len(out_entries) == 4:  # TODO: currently 4 is tied to spark, untie this
            # parse the entries
            # dict_train = parser.parse_log(info, in_entries, out_entries)
            try:
                dict_train = parser.parse_log(info, in_entries, out_entries)
            except Exception as e:
                # raise spark_utils.AnalysisException
                print("Parsing error, skip this log.", e)
                continue

            # decide the minimum feature set along the way; this assumes the min feature set
            # stays the same across different log partitions. TODO: allow min_feature_set to
            # be aggregated across partitions, e.g., each partition should dump a unique mini
            # feature set and all sets are reduced once all parsed.
            min_feature_set = set(dict_train.keys()) if len(min_feature_set) == 0 \
                else min_feature_set.intersection(dict_train.keys())

            # TODO: replace this complex min_feature_set with an offline linear scan to the featuredb;
            # which checks whether there are (hopefully) a few rows that have different set of features,
            # if so, remove those rows. It has concurrency issue, addressed by having multiple min_feature_set
            # in each partition.
            min_feature_set_by_app[app_id] = min_feature_set

            app_rows[log_id] = utils.enforce_str_type(dict_train)

        last_app_id = app_id

    if len(app_rows) > 0:
        mongo.put_training_data(last_app_id, app_rows)
    return min_feature_set_by_app


def get_entries(log_index: dict, db_client: featuredb.FeatureDBClient = None, log_bucket=oracle_cg.s3_log_bucket) -> Generator:
    """Generate samples for a given log index app by app."""
    bucket = log_bucket

    for app_id, logs in log_index.items():
        if in_blacklist(app_id) or len(logs) == 0:
            continue

        parsed = set() if db_client is None \
            else get_parsed_log_index(db_client, app_id)

        for i, gen_ in enumerate(logs.items()):
            id_, log = gen_
            id_ = normalized_feature_name(id_)

            info, in_entries, out_entries = dict(), dict(), dict()

            if id_ in parsed:
                print("log already parsed; skipped.")
                continue

            print("\npreparing {0}/{1} row for {2}\n".format(i + 1, len(logs), app_id))

            for entry in log:
                obj = s3.get_object(bucket, prefix=entry.key)
                if entry.entry_type == "input":
                    in_entries[entry.name] = obj
                if entry.entry_type == "output":
                    out_entries[entry.name] = obj
                if entry.entry_type == "info":
                    info = json.loads(obj)

            yield app_id, id_, info, in_entries, out_entries


def reduce_and_put_min_feature_sets(min_feature_sets_by_app):
    # app_rows["min_feature_set"] = {k: 1 for k in min_feature_set}
    results = dict()
    mongo = featuredb.FeatureDBClient()

    for m_by_a in min_feature_sets_by_app:
        for a, m in m_by_a.items():
            if a not in results:
                results[a] = m
            else:
                results[a] = results[a].intersection(m, results[a])

    for a, m in results.items():
        mongo.put_training_data(a, {"min_feature_set": {k: 1 for k in m}})


def gen_training_data_parallel(log_index: dict, num_partition: int, batch_size: int, *, parser):
    """Splits the log_index into partitions and parse logs on each partition.
    The partitioning is done  over log index. The log_index structure is
    app_id -> log_id -> entry_index. Note that the actual number of logs to
    be parsed is limited by num_partition * limit."""

    parts = partition_log_index(log_index, num_partition)
    min_feature_sets_by_app = ray.get([gen_training_data_func.remote(index_part, batch_size, parser)
                                       for index_part in parts])
    reduce_and_put_min_feature_sets(min_feature_sets_by_app)


@ray.remote
def gen_training_data_func(log_index: dict, batch_size: int, parser):
    """A wrapper function for gen_training_data for ray invocation.

    Ray does not seem to support serializing dataframes, so use dict when necessary.
    """
    return gen_training_data(log_index, batch_size, parser)


def gen_samples(log_index: dict, num_sample: int, log_bucket=oracle_cg.s3_log_bucket) -> List:
    samples = get_entries(log_index, log_bucket=log_bucket)
    return [next(samples) for _ in range(num_sample)]


def get_parsed_log_index(db_client: featuredb.FeatureDBClient, app_id: str) -> set:
    """Return the index of parsed logs for a given app."""
    index = set()
    from collections import defaultdict
    counter = defaultdict(int)
    for k, v in db_client.get_training_data(app_id):
        index.add(k)
        counter[v["API_Name"]] += 1
    return index


def partition_log_index(root_index: dict, num: int) -> List[dict]:
    """Randomly partitioned the index into given number of partitions and return
    the list of them."""
    partitions = [{} for _ in range(num)]
    total_size = sum([len(app_index.keys()) for app_index in root_index.values()])
    if total_size < num:
        partition_size = total_size
    else:
        partition_size = int(round(total_size / num))
    for app_id, app_index in root_index.items():
        shuffled_keys = list(app_index.keys())
        random.shuffle(shuffled_keys)
        for i, p in enumerate(partitions):
            p[app_id] = {k: app_index[k]
                         for k in shuffled_keys[i * partition_size:(i + 1) * partition_size]}
    return partitions


def in_blacklist(app_id):
    return app_id in {
        # "spark-org.apache.spark.examples.SparkMultiBroadcastTest",
        # "spark-org.apache.spark.examples.SparkALS",
        # TODO: fix the JVM OOM with parsing SparkTC logs
        # "spark-org.apache.spark.examples.SparkTC",
    }


def main():
    from oracle.feature.spark.gen import get_spark_log_index

    ray.init()
    gen_training_data_parallel(get_spark_log_index(),
                               num_partition=8,
                               batch_size=1000,
                               parser="spark")


if __name__ == '__main__':
    main()
