"""Tests for Bronze job validation-before-publication ordering."""

from __future__ import annotations

from unittest import TestCase
from unittest.mock import MagicMock, patch

from spark.bronze.job import (
    BronzeJobResult,
    run_bronze_batch,
    run_bronze_job,
)
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

    @patch("spark.bronze.job.run_bronze_job")
    def test_batch_processes_tables_in_registry_order(
        self,
        run_table: MagicMock,
    ) -> None:
        run_table.side_effect = [
            BronzeJobResult(
                table_name=table,
                batch_id="batch-1",
                input_path=f"raw/{table}",
                output_path=f"bronze/{table}",
                raw_count=1,
                bronze_count=1,
            )
            for table in ("business_units", "departments")
        ]

        results = run_bronze_batch(
            MagicMock(),
            table_names=("business_units", "departments"),
            batch_id="batch-1",
            verify_existing=True,
        )

        self.assertEqual(
            [result.table_name for result in results],
            ["business_units", "departments"],
        )
        self.assertEqual(
            [call.kwargs["table_name"] for call in run_table.call_args_list],
            ["business_units", "departments"],
        )
        self.assertTrue(
            all(
                call.kwargs["verify_existing"]
                for call in run_table.call_args_list
            )
        )

    @patch("spark.bronze.job.write_bronze")
    @patch("spark.bronze.job.validate_bronze")
    @patch("spark.bronze.job.bronze_output_exists", return_value=True)
    @patch("spark.bronze.job.read_raw_table")
    def test_existing_output_is_revalidated_without_publication(
        self,
        read_raw_table: MagicMock,
        output_exists: MagicMock,
        validate_bronze: MagicMock,
        write_bronze: MagicMock,
    ) -> None:
        spark = MagicMock()
        raw_dataframe = MagicMock()
        batch = make_batch()
        read_raw_table.return_value = (raw_dataframe, batch)
        validate_bronze.return_value = MagicMock(raw_count=1, bronze_count=1)

        result = run_bronze_job(
            spark,
            table_name="business_units",
            batch_id="batch-1",
            verify_existing=True,
        )

        self.assertEqual(result.publication_status, "existing_verified")
        output_exists.assert_called_once()
        validate_bronze.assert_called_once_with(
            raw_dataframe,
            spark.read.parquet.return_value,
            batch,
        )
        write_bronze.assert_not_called()
