import importlib.resources
import unittest

from pyspark import Row
from pyspark.sql import DataFrame

from app.detector import (
    load_tracking,
    detect_speeding_events,
    predict_speeding_event,
)


class TestDetector(unittest.TestCase):
    TEST_TYPE = "test"
    DATA = "sample.jsonl"
    HORIZON = 10

    @classmethod
    def setUpClass(cls) -> None:
        cls.speeding_events = cls._get_speeding_events(cls.DATA).cache()
        cls.predictions = cls._get_predictions(cls.DATA, cls.HORIZON).cache()

    @classmethod
    def _get_logs(cls, filename: str) -> DataFrame:
        with importlib.resources.path(cls.TEST_TYPE, "resources") as p:
            return load_tracking(p / filename)

    @classmethod
    def _get_speeding_events(cls, filename: str) -> DataFrame:
        return detect_speeding_events(cls._get_logs(filename))

    @classmethod
    def _get_predictions(cls, filename: str, horizon: int = 10) -> DataFrame:
        return predict_speeding_event(cls._get_speeding_events(filename), horizon)

    def _get_nth_speeding_event(self, n: int) -> Row:
        results = (
            self.speeding_events.filter("is_speeding == True")
            .orderBy("timespan", "customer_id", "driver_id", "vehicle_id")
            .collect()
        )
        try:
            return results[n]
        except IndexError:
            self.fail("Row not found.")

    def _get_nth_valid_prediction(self, n: int) -> Row:
        results = (
            self.predictions.filter("will_be_speeding == True")
            .filter("actually_speeding == True")
            .orderBy("timespan", "customer_id", "driver_id", "vehicle_id")
            .collect()
        )
        try:
            return results[n]
        except IndexError:
            self.fail("Row not found.")

    def _get_nth_invalid_prediction(self, n: int) -> Row:
        results = (
            self.predictions.filter("will_be_speeding == True")
            .filter("actually_speeding == False")
            .orderBy("timespan", "customer_id", "driver_id", "vehicle_id")
            .collect()
        )
        try:
            return results[n]
        except IndexError:
            self.fail("Row not found.")

    def test_detect_speeding_events_adds_is_speeding_column(self):
        self.assertTrue("is_speeding" in self.speeding_events.columns)

    def test_detect_speeding_events_detects_valid_number_of_events(self):
        self.assertEqual(self.speeding_events.filter("is_speeding == True").count(), 1)

    def test_detect_speeding_events_detects_valid_speeding_event(self):
        event = self._get_nth_speeding_event(0)
        self.assertAlmostEqual(event["location_x"], 65.1861835687)
        self.assertAlmostEqual(event["location_y"], 6.9766616674)

    def test_predict_speeding_event_adds_actually_speeding_column(self):
        self.assertTrue("actually_speeding" in self.predictions.columns)

    def test_predict_speeding_event_finds_valid_number_of_valid_predictions(self):
        valid_predictions = (
            self.predictions.filter("will_be_speeding == True")
            .filter("actually_speeding == True")
            .count()
        )
        self.assertEqual(valid_predictions, 1)

    def test_predict_speeding_event_finds_valid_predictions(self):
        prediction = self._get_nth_valid_prediction(0)
        self.assertAlmostEqual(prediction["location_x"], 50.0611120179)
        self.assertAlmostEqual(prediction["location_y"], -11.8692303161)

    def test_predict_speeding_event_finds_valid_number_of_invalid_predictions(self):
        invalid_predictions = (
            self.predictions.filter("will_be_speeding == True")
            .filter("actually_speeding == False")
            .count()
        )
        self.assertEqual(invalid_predictions, 1)

    def test_predict_speeding_event_finds_invalid_predictions(self):
        prediction = self._get_nth_invalid_prediction(0)
        self.assertAlmostEqual(prediction["location_x"], 123.2485672195)
        self.assertAlmostEqual(prediction["location_y"], 91.7736567415)
