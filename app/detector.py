from functools import lru_cache, reduce
from os import PathLike

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@lru_cache(maxsize=1)
def get_spark():
    sc = SparkContext(master="local[1]", appName="Speeding detector")
    spark = SparkSession(sc)
    return spark


def load_tracking(tracking_path: PathLike) -> DataFrame:
    return get_spark().read.json(str(tracking_path))


# TODO: Task #1
def get_same_ride() -> pyspark.sql.window.WindowSpec:
    """Returns a window frame and ordered by timespan.
    NOTE: the same_ride has always the same columns and ordering"""
    return Window.partitionBy('customer_id', 'vehicle_id', 'driver_id').orderBy('timespan')


def add_previous_location_and_time(logs: DataFrame, window) -> DataFrame:
    """Adds columns with the previous location and time information."""
    cols_dict = {'location_x': 'prev_x',
                 'location_y': 'prev_y',
                 'timespan': 'prev_timespan'}
    return logs\
        .withColumn('prev_x', F.lag('location_x').over(window)) \
        .withColumn('prev_y', F.lag('location_y').over(window)) \
        .withColumn('prev_timespan', F.lag('timespan').over(window))


def calculate_distance(logs: DataFrame) -> DataFrame:
    """Calculates the Euclidean distance between the current and previous location."""
    return logs.withColumn("distance_km",
                           F.sqrt(F.pow(logs["location_x"] - logs["prev_x"], 2) +
                                  F.pow(logs["location_y"] - logs["prev_y"], 2)))


def calculate_time_delta(logs: DataFrame, SECONDS_PER_HOUR: int) -> DataFrame:
    """Calculates the time delta between the current and previous location in hours."""
    return logs.withColumn("time_hours",
                           (logs["timespan"] - logs["prev_timespan"]) / SECONDS_PER_HOUR)


def calculate_speed(logs: DataFrame) -> DataFrame:
    """Calculate the speed = distance_km / time_hours"""
    return logs.withColumn("speed", logs["distance_km"] / logs["time_hours"])


def detect_speeding_events(logs: DataFrame) -> DataFrame:
    """Skeleton of template method"""
    SECONDS_PER_HOUR = 3600
    window = get_same_ride()

    logs = add_previous_location_and_time(logs, window)
    logs = calculate_distance(logs)
    logs = calculate_time_delta(logs, SECONDS_PER_HOUR)
    logs = calculate_speed(logs)
    return logs.withColumn("is_speeding", (logs["speed"] > logs["speed_limit"]))


# TODO: Task #2
def predict_speeding_event(
        logs_with_speeding: DataFrame, prediction_horizon: int
) -> DataFrame:
    # Code readability is improved by using variables for the column names,
    # making it easier to maintain the code if the column names change in the future
    same_ride_cols = 'customer_id, driver_id, vehicle_id'
    is_speeding_col = 'is_speeding'
    time_col = 'timespan'

    logs_with_speeding.createOrReplaceTempView("logs_with_speeding")
    query = f"""
        SELECT *,
               MAX({is_speeding_col}) OVER (PARTITION BY {same_ride_cols}
                                            ORDER BY {time_col}
                                            ROWS BETWEEN 1 FOLLOWING AND {prediction_horizon} FOLLOWING) as actually_speeding
        FROM logs_with_speeding
    """
    return get_spark().sql(query)


if __name__ == '__main__':
    df = get_spark().read.json("test/resources/sample.jsonl")
    df.printSchema()
    df.show()

    # 1
    df = detect_speeding_events(df)
    df.show(n=50)

    # 2
    df = predict_speeding_event(logs_with_speeding=df, prediction_horizon=10)
    df.show(n=50)
