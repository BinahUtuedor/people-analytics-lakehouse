"""Tests for Bronze job validation-before-publication ordering."""

from __future__ import annotations

from unittest import TestCase
from unittest.mock import MagicMock, patch

from spark.bronze.job import run_bronze_job
from spark.bronze.validate import BronzeValidationError
from tests.spark.bronze.test_transform import make_batch


class BronzeJobTests(TestCase):
    @patch("spark.bronze.job.write_bronze")
    @patch("spark.bronze.job.validate_bronze")
    @patch("spark.bronze.job.transform_to_bronze")
    @patch("spark.bronze.job.read_raw_table")
    def test_validation_failure_prevents_publication(
        self,
        read_raw_table: MagicMock,
        transform_to_bronze: MagicMock,
        validate_bronze: MagicMock,
        write_bronze: MagicMock,
    ) -> None:
        raw_dataframe = MagicMock()
        bronze_dataframe = MagicMock()
        read_raw_table.return_value = (raw_dataframe, make_batch())
        transform_to_bronze.return_value = bronze_dataframe
        validate_bronze.side_effect = BronzeValidationError("invalid")

        with self.assertRaises(BronzeValidationError):
            run_bronze_job(
                MagicMock(),
                table_name="business_units",
                batch_id="batch-1",
                bucket="unit-test-bucket",
            )
        write_bronze.assert_not_called()
