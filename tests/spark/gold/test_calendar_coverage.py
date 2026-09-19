"""Reference month-end coverage must never extend business eligibility."""

from dataclasses import replace
from datetime import date
from pyspark.sql import functions as F
from spark.gold.transform import build_dim_date
from spark.gold.workforce_monthly import closed_months
from tests.spark.gold.dimension_fixtures import CoreDimensionTestCase


class CalendarCoverageTests(CoreDimensionTestCase):
    def test_calendar_boundaries(self):
        for cutoff, end in (
            (date(2024, 4, 15), date(2024, 4, 30)),
            (date(2024, 4, 30), date(2024, 4, 30)),
            (date(2024, 2, 15), date(2024, 2, 29)),
            (date(2023, 2, 15), date(2023, 2, 28)),
            (date(2024, 12, 1), date(2024, 12, 31)),
        ):
            with self.subTest(cutoff=cutoff):
                start = cutoff.replace(day=1)
                build = replace(
                    self.build,
                    spec=replace(
                        self.build.spec,
                        reporting_start=start,
                        reporting_end=cutoff,
                        source_cutoff=cutoff,
                    ),
                )
                frame = build_dim_date(self.spark, build)
                real = frame.filter(~F.col("is_unknown"))
                rows = real.collect()
                self.assertEqual(len(rows), end.day)
                self.assertEqual(min(r.calendar_date for r in rows), start)
                self.assertEqual(max(r.calendar_date for r in rows), end)
                self.assertIn(
                    end.year * 10000 + end.month * 100 + end.day,
                    {r.date_key for r in rows},
                )
                self.assertEqual(
                    closed_months(frame, build).count(), int(cutoff == end)
                )
