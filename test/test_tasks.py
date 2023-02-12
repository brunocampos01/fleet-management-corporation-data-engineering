import importlib.resources
import unittest
import pyspark

from pyspark.sql.window import Window
from pyspark import Row
from pyspark.sql import DataFrame

from app.detector import (
    load_tracking,
    detect_speeding_events,
    predict_speeding_event,
    get_same_ride,
    add_previous_location_and_time,
    calculate_distance,
    calculate_time_delta,
    calculate_speed,
)


class TestTasks(unittest.TestCase):
    TEST_TYPE = "test"
    DATA = "sample.jsonl"
    HORIZON = 10

    @classmethod
    def setUpClass(cls) -> None:
        # cls.speeding_events = cls._get_speeding_events(cls.DATA).cache()
        cls.df = cls._get_logs(cls.DATA)

    @classmethod
    def _get_logs(cls, filename: str) -> DataFrame:
        with importlib.resources.path(cls.TEST_TYPE, "resources") as p:
            return load_tracking(p / filename)

    # task 01
    def test_get_same_ride(self):
        window = get_same_ride()
        self.assertIsInstance(window, pyspark.sql.window.WindowSpec)

    # def test_add_previous_location_and_time(self):
    #     window = get_same_ride()
    #     logs = add_previous_location_and_time(self.df, window)
    #
    #     self.assertIsInstance(logs, DataFrame)
    #
    # def test_calculate_distance(self):
    #     window = get_same_ride()
    #     logs = add_previous_location_and_time(self.df, window)
    #     logs = calculate_distance(logs)
    #
    #     self.assertIsInstance(logs, DataFrame)
    #
    # def test_calculate_time_delta(self):
    #     window = get_same_ride()
    #     logs = add_previous_location_and_time(self.df, window)
    #     logs = calculate_distance(logs)
    #     logs = calculate_time_delta(logs, 3600)
    #
    #     self.assertIsInstance(logs, DataFrame)
    #
    # def test_calculate_speed(self):
    #     window = get_same_ride()
    #     logs = add_previous_location_and_time(self.df, window)
    #     logs = calculate_distance(logs)
    #     logs = calculate_time_delta(logs, 3600)
    #     logs = calculate_speed(logs)
    #
    #     self.assertIsInstance(logs, DataFrame)

    def test_detect_speeding_events(self):
        window = get_same_ride()
        logs = add_previous_location_and_time(self.df, window)
        logs = calculate_distance(logs)
        logs = calculate_time_delta(logs, 3600)
        logs = calculate_speed(logs)
        logs = detect_speeding_events(logs)

        self.assertTrue("prev_x" in logs.columns)
        self.assertTrue("prev_y" in logs.columns)
        self.assertTrue("prev_timespan" in logs.columns)
        self.assertTrue("distance_km" in logs.columns)
        self.assertTrue("time_hours" in logs.columns)
        self.assertTrue("speed" in logs.columns)
        self.assertTrue("is_speeding" in logs.columns)

        self.assertIsInstance(logs, DataFrame)
