import unittest

import pandas as pd

from etl import validate_air_quality


class ValidateAirQualityTests(unittest.TestCase):
    def test_missing_pm10_is_rejected_by_timestamp(self):
        readings = pd.DataFrame([
            {
                "city_name": "Kochi",
                "obs_time": "2026-11-24T00:00",
                "pm10": 20,
                "pm2_5": 10,
                "us_aqi": 50,
            },
            {
                "city_name": "Kochi",
                "obs_time": "2026-11-24T01:00",
                "pm10": None,
                "pm2_5": 11,
                "us_aqi": 51,
            },
            {
                "city_name": "Kochi",
                "obs_time": "2026-11-24T02:00",
                "pm10": 30,
                "pm2_5": 12,
                "us_aqi": None,
            },
        ])

        clean, rejected = validate_air_quality(readings)

        self.assertEqual(len(clean), 2)
        self.assertEqual(set(clean["obs_time"]), {
            "2026-11-24 00:00:00",
            "2026-11-24 02:00:00",
        })
        self.assertEqual(len(rejected), 1)
        rejected_row = rejected.iloc[0]
        self.assertEqual(rejected_row["obs_time"], "2026-11-24 01:00:00")
        self.assertTrue(pd.isna(rejected_row["pm10"]))
        self.assertIn("missing_pm10;", rejected_row["reason"])

    def test_invalid_pm_and_aqi_are_rejected(self):
        readings = pd.DataFrame([
            {
                "city_name": "Chennai",
                "obs_time": "2026-11-24T00:00",
                "pm10": -1,
                "pm2_5": 10,
                "us_aqi": 50,
            },
            {
                "city_name": "Chennai",
                "obs_time": "2026-11-24T01:00",
                "pm10": 20,
                "pm2_5": 10,
                "us_aqi": 501,
            },
        ])

        clean, rejected = validate_air_quality(readings)

        self.assertTrue(clean.empty)
        self.assertEqual(len(rejected), 2)
        reasons_by_time = dict(zip(rejected["obs_time"], rejected["reason"]))
        self.assertIn("negative_pm;", reasons_by_time["2026-11-24 00:00:00"])
        self.assertIn("aqi_out_of_range;", reasons_by_time["2026-11-24 01:00:00"])


if __name__ == "__main__":
    unittest.main()
