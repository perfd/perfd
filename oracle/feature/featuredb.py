import time

from pymongo import MongoClient
from pymongo import errors

from pyspark.sql import SparkSession, DataFrame, Row

import oracle.configs as ocg
import oracle.utils as utils
from oracle.utils import cmd, timed

from oracle.feature.utils import normalized_feature_name

try:
    from oracle.experiments.__cred__ import MONGO_USER, MONGO_PASSWD

    _mongo_user = MONGO_USER
    _mongo_passwd = MONGO_PASSWD
except (ModuleNotFoundError, ImportError):
    # TODO: use config file in place of __cred__
    print("no mongod user or passwd given, check if __cred__ module exist.")
    raise


class FeatureDBClient:
    """Provide access to log database.

    Each application's training data stored in a collection.
    """

    def __init__(self, db_name=ocg.local_db_name):
        self.db_name = db_name

    def __getattr__(self, item):
        if item in {"db_client", "db"}:
            try:
                self.db_client, self.db = self.connect_db()
            except errors.ServerSelectionTimeoutError:
                self.start_db_daemon()
                self.db_client, self.db = self.connect_db()

            print("--> connected to log db:", self.db_name)
            return self.__dict__[item]

    def connect_db(self):
        db_client = MongoClient("localhost",
                                # username=_mongo_user,
                                # password=_mongo_passwd,
                                authSource=self.db_name,
                                # authMechanism="SCRAM-SHA-1",
                                )
        return db_client, db_client[self.db_name]

    @staticmethod
    def start_db_daemon():
        cmd("mongod  --bind_ip 127.0.0.1 --auth --port 27017 --dbpath {} "
            ">> /tmp/mongo.log 2>&1 &".format(ocg.local_db_dir))

    @staticmethod
    def encode_key(row_id: str):
        """Mongodb does not support '.' in key name. Sad ):"""
        return normalized_feature_name(row_id)

    @staticmethod
    def decode_key(row_id: str):
        return row_id

    @staticmethod
    def encode_row(row: dict):
        return {FeatureDBClient.encode_key(k): v for k, v in row.items()}

    @staticmethod
    def decode_row(row: dict, feature_gate=None):
        return {FeatureDBClient.decode_key(k): v for k, v in row.items()
                if feature_gate is None or k in feature_gate}

    def put_training_data(self, app_id: str, train_rows: dict, transform=lambda x: x):
        """Insert the rows of training data to the db where each row is a document.

        A row can be transformed before insertion by the supplied transform method."""
        collection = self.db[app_id]
        to_insert = [{self.encode_key(row_id): self.encode_row(transform(row))} for row_id, row in train_rows.items()]

        # print("putting:", app_id, len(to_insert), len(train_rows), list(train_rows.keys()))
        if len(to_insert) > 0:
            collection.insert_many(to_insert)

    def get_training_data(self, app_id: str, transform=lambda x: x, feature_gate=None) -> dict:
        """Given the app_id, returns the documents in the corresponding collection."""
        collection = self.db[app_id]

        for obj in collection.find(projection={'_id': False}):
            k, v = list(obj.items())[0]
            if k == "min_feature_set":
                continue
            yield self.decode_key(k), transform(self.decode_row(v, feature_gate))

    def get_min_feature_set(self, app_id: str):
        # print(self.db[app_id].count())
        return self.db[app_id].find_one({"min_feature_set": {"$exists": True}})["min_feature_set"]

    def list_collections(self):
        return self.db.list_collection_names()


# @timed
def load_training_data_as_df(app_id, *, db_name=ocg.local_db_name, spark=None, db_client=None) -> DataFrame:
    """Return the training data as data frames."""
    spark = SparkSession.builder \
        .master("local") \
        .appName("microps") \
        .getOrCreate() if spark is None else spark
    sc = spark.sparkContext

    mongo = FeatureDBClient(db_name) if db_client is None else db_client
    app_rows = dict()

    # print(mongo.list_collections())
    min_feature_set = mongo.get_min_feature_set(app_id)

    start, skip_count = time.time(), 0
    for k, v in mongo.get_training_data(app_id=app_id,
                                        transform=lambda x: Row(**utils.enforce_str_type(x)),
                                        feature_gate=min_feature_set):
        # feature_gate=min_feature_set):
        if len(v) == len(min_feature_set):
            # if True:
            app_rows[k] = v
        else:
            # print(set(min_feature_set) - set(v.asDict().keys()))
            skip_count += 1

    app_rows = list(app_rows.values())
    rdd = sc.parallelize(app_rows)

    if skip_count > 0:
        print("skip %d row due to having inconsistent features than min_feature_set" % skip_count)
    print("done loading", time.time() - start)
    return spark.createDataFrame(rdd)


load_training_data = load_training_data_as_df


def filter_by_feature_gate(entries: dict, feature_gates: set):
    for fg in feature_gates:
        del entries[fg]


def remove_training_data(app_id, *, db_name=ocg.local_db_name):
    mongo = FeatureDBClient(db_name)
    mongo.db[app_id].drop()
    print("training data of {} removed.".format(app_id))


if __name__ == '__main__':
    # TODO: add unit_test
    # df = load_training_data(app_id="spark-org.apache.spark.examples.SparkLR", db_name="microps")
    df = load_training_data(app_id="spark-org.apache.spark.examples.SparkLR")
    df.printSchema()
    print(df.count(), len(df.columns))
    # df.write.csv('full.csv', mode="overwrite", header=True)
