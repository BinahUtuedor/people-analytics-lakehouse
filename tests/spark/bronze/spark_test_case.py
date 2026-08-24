"""Shared local Spark test fixture."""

from __future__ import annotations

import os
import unittest

from pyspark.sql import SparkSession


class SparkTestCase(unittest.TestCase):
    """Start one small local Spark session for a test class."""

    spark: SparkSession

    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("people-analytics-bronze-unit-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
