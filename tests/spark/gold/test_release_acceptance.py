"""R01: generic inventory/diagnostic construction cannot authorize promotion.

Pattern B intentionally has no successful promotion API. These tests exercise
both public constructors; existing Spark/writer tests verify actual bad data.
"""

from dataclasses import replace
from unittest import TestCase
from unittest.mock import patch

from spark.gold.manifest import (
    ReleaseManifest,
    ReleaseStatus,
    VERIFICATION_CHECKS,
)
from spark.gold.release_validation import build_release_manifest
from tests.spark.gold.test_manifest import STAMP, inventory, spec


class ReleaseAcceptanceTests(TestCase):
    def claims(self, **changes):
        values = dict(
            spec=spec(),
            generated_at=STAMP,
            models=inventory(True),
            status=ReleaseStatus.ACCEPTED,
            verification_outcomes={name: True for name in VERIFICATION_CHECKS},
            reconciliation_metrics={"cross_model": "passed"},
        )
        values.update(changes)
        return values

    def reject_claims(self, **changes):
        for status in (ReleaseStatus.VALIDATED, ReleaseStatus.ACCEPTED):
            values = self.claims(**changes, status=status)
            with (
                self.subTest(status=status),
                self.assertRaisesRegex(ValueError, "Evidence-backed promotion"),
            ):
                ReleaseManifest(**values)
            # A forbidden promotion must fail before Spark actions or IO,
            # regardless of whether the frames/evidence could be inspected.
            with (
                patch(
                    "spark.gold.release_validation.release_reconciliation_report"
                ) as report,
                self.assertRaisesRegex(ValueError, "BUILDING-only"),
            ):
                build_release_manifest(
                    {},
                    None,
                    STAMP,
                    status=status,
                    verification_outcomes=values["verification_outcomes"],
                    reconciliation_metrics=values["reconciliation_metrics"],
                )
            report.assert_not_called()

    def test_caller_success_flags_cannot_promote(self):
        self.reject_claims()

    def test_cross_model_failed_cannot_promote(self):
        self.reject_claims(reconciliation_metrics={"cross_model": "FAILED"})

    def test_physical_readback_not_performed_cannot_promote(self):
        self.reject_claims(
            reconciliation_metrics={"physical_readback": "not performed"}
        )

    def test_failed_or_missing_reconciliation_cannot_promote(self):
        for result in ("pending", "failed", "not performed", "missing"):
            with self.subTest(result=result):
                self.reject_claims(reconciliation_metrics={"reconciliation": result})

    def test_primary_and_secondary_duplicate_claims_cannot_promote(self):
        for grain in ("primary_key", "secondary_grain"):
            with self.subTest(grain=grain):
                self.reject_claims(reconciliation_metrics={grain + "_duplicates": "1"})

    def test_another_builds_claims_cannot_promote(self):
        other = replace(spec(), silver_batch_id="other-batch")
        self.assertNotEqual(other.build_id, spec().build_id)
        self.reject_claims(
            reconciliation_metrics={"validation_build_id": other.build_id}
        )

    def test_missing_model_cannot_promote(self):
        self.reject_claims(models=inventory(True)[:-1])
        with self.assertRaisesRegex(ValueError, "exactly all ten"):
            ReleaseManifest(spec(), STAMP, inventory()[:-1])

    def test_missing_partition_cannot_promote(self):
        models = tuple(
            replace(m, partitions=(), row_count=0) if m.model == "fact_payroll" else m
            for m in inventory(True)
        )
        self.reject_claims(models=models)

    def test_failed_physical_evidence_cannot_promote(self):
        for failure in (
            "missing output",
            "missing partition",
            "unexpected partition",
            "schema corruption",
            "metadata corruption",
            "content/hash mismatch",
            "mixed build",
            "immutable destination violation",
            "_SUCCESS only",
        ):
            with self.subTest(failure=failure):
                self.reject_claims(
                    reconciliation_metrics={"physical_readback": failure}
                )

    def test_building_to_accepted_and_validated_are_unavailable(self):
        original = ReleaseManifest(spec(), STAMP, inventory())
        for status in (ReleaseStatus.VALIDATED, ReleaseStatus.ACCEPTED):
            with self.assertRaisesRegex(ValueError, "Evidence-backed promotion"):
                replace(original, status=status)
        self.assertEqual(original.status, ReleaseStatus.BUILDING)

    def test_failed_to_accepted_is_unavailable(self):
        failed = ReleaseManifest(
            spec(),
            STAMP,
            inventory(),
            status=ReleaseStatus.FAILED,
            verification_outcomes={"reconciliation": False},
            reconciliation_metrics={"reconciliation": "failed"},
        )
        with self.assertRaisesRegex(ValueError, "Evidence-backed promotion"):
            replace(
                failed,
                **{
                    k: v
                    for k, v in self.claims().items()
                    if k not in ("spec", "generated_at", "models")
                },
            )
        self.assertEqual(failed.status, ReleaseStatus.FAILED)

    def test_validated_claim_cannot_bypass_missing_physical_evidence(self):
        self.reject_claims(
            verification_outcomes={
                name: name != "physical_readback" for name in VERIFICATION_CHECKS
            },
            reconciliation_metrics={"logical_status": "VALIDATED"},
        )

    def test_complete_claims_remain_building_and_identity_is_unchanged(self):
        original = ReleaseManifest(spec(), STAMP, inventory())
        diagnostic = ReleaseManifest(**self.claims(status=ReleaseStatus.BUILDING))
        self.assertEqual(diagnostic.status, ReleaseStatus.BUILDING)
        self.assertEqual(diagnostic.spec.build_id, original.spec.build_id)
        self.assertEqual(diagnostic.to_dict()["status"], "BUILDING")

    def test_factory_rejects_every_non_building_state(self):
        for status in (
            ReleaseStatus.FAILED,
            ReleaseStatus.VALIDATED,
            ReleaseStatus.ACCEPTED,
            "BUILDING",
            "ACCEPTED",
        ):
            with (
                self.subTest(status=status),
                self.assertRaisesRegex(ValueError, "BUILDING-only"),
            ):
                build_release_manifest({}, None, STAMP, status=status)
