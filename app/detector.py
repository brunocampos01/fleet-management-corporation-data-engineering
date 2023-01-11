from functools import lru_cache
from os import PathLike

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit


@lru_cache(maxsize=1)
def get_spark():
    sc = SparkContext(master="local[1]", appName="Speeding detector")
    spark = SparkSession(sc)
    return spark


def load_tracking(tracking_path: PathLike) -> DataFrame:
    return get_spark().read.json(str(tracking_path))


# TODO: Task #1
def detect_speeding_events(logs: DataFrame) -> DataFrame:
    return logs.withColumn("is_speeding", lit(False))


# TODO: Task #2
def predict_speeding_event(
    logs_with_speeding: DataFrame, prediction_horizon: int
) -> DataFrame:
    return logs_with_speeding.withColumn("actually_speeding", lit(False))
