"""Build identity, release inventory and no-repair policy tests."""

from dataclasses import replace
from datetime import date, datetime, timezone, timedelta
import json
from unittest import TestCase
from spark.gold.contracts import GOLD_MODELS
from spark.gold.manifest import (
    BuildSpec,
    RuntimeContract,
    ModelInventory,
    ReleaseManifest,
    ReleaseStatus,
    RunMode,
    ExistingOutputError,
    VerificationRequest,
    VERIFICATION_CHECKS,
    require_output_policy,
)

STAMP = datetime(2024, 3, 1, tzinfo=timezone.utc)


def spec():
    sources = {s for m in GOLD_MODELS.values() for s in m.silver_sources}
    return BuildSpec(
        "batch-a",
        "hr",
        {s: "a" * 64 for s in sources},
        "revision-a",
        date(2024, 1, 1),
        date(2024, 2, 29),
        date(2024, 2, 29),
    )


def inventory(evidence=False):
    return tuple(
        ModelInventory(
            name,
            ((("reporting_year", 2024),),) if model.partition_fields else ((),),
            (1 if model.unknown_member else 0) if evidence else None,
            "b" * 64 if evidence else None,
        )
        for name, model in GOLD_MODELS.items()
    )


class BuildTests(TestCase):
    def test_deterministic_and_order_independent(self):
        original = spec()
        changed = replace(
            original,
            input_fingerprints=dict(
                reversed(list(original.input_fingerprints.items()))
            ),
            assumption_policies=tuple(reversed(original.assumption_policies)),
        )
        self.assertEqual(original.build_id, changed.build_id)
        self.assertRegex(original.build_id, r"^[a-f0-9]{64}$")

    def test_material_inputs_and_every_contract_version(self):
        original = spec()
        changes = {
            "silver_batch_id": "batch-b",
            "source_namespace": "hr-2",
            "code_revision": "revision-b",
            "reporting_start": date(2024, 1, 2),
            "reporting_end": date(2024, 2, 28),
            "source_cutoff": date(2024, 3, 1),
            "schema_version": "v2",
            "key_version": "v2",
            "serialization_version": "v2",
            "manifest_version": "v2",
            "assumption_policies": ("future-policy",),
            "runtime": RuntimeContract(ansi_enabled=False),
            "input_fingerprints": {
                **original.input_fingerprints,
                "employees": "c" * 64,
            },
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                self.assertNotEqual(
                    original.build_id, replace(original, **{field: value}).build_id
                )

    def test_execution_timestamp_is_outside_identity(self):
        first = ReleaseManifest(spec(), STAMP, inventory())
        second = replace(first, generated_at=STAMP + timedelta(days=1))
        self.assertEqual(first.spec.build_id, second.spec.build_id)
        self.assertNotEqual(first.to_json(), second.to_json())
        for name in (
            "execution_timestamp",
            "hostname",
            "local_root",
            "process_id",
            "temporary_directory",
        ):
            self.assertNotIn(name, first.spec.to_dict())
            with self.assertRaises(TypeError):
                replace(first.spec, **{name: "unapproved"})

    def test_input_validation_and_mutation_protection(self):
        original = spec()
        for changes in (
            {"silver_batch_id": ""},
            {"input_fingerprints": {}},
            {"reporting_end": date(2025, 1, 1)},
            {"reporting_start": "2024-01-01"},
            {"assumption_policies": ()},
            {"assumption_policies": ("same", "same")},
            {
                "input_fingerprints": {
                    **original.input_fingerprints,
                    "employees": "path/file.parquet",
                }
            },
        ):
            with self.assertRaises(ValueError):
                replace(original, **changes)
        with self.assertRaises(TypeError):
            original.input_fingerprints["employees"] = "d" * 64


class ManifestTests(TestCase):
    def test_deterministic_json_and_required_fields(self):
        manifest = ReleaseManifest(spec(), STAMP, inventory())
        reordered = replace(manifest, models=tuple(reversed(manifest.models)))
        self.assertEqual(manifest.to_json(), reordered.to_json())
        value = json.loads(manifest.to_json())
        self.assertEqual(value["build_id"], manifest.spec.build_id)
        self.assertEqual(value["status"], "BUILDING")
        self.assertEqual(len(value["expected_models"]), 10)
        self.assertTrue(
            all(
                m["schema_contract_id"].startswith("gold:v1:")
                for m in value["expected_models"]
            )
        )
        self.assertEqual(value["generated_at"], "2024-03-01T00:00:00.000000Z")
        self.assertEqual(
            manifest.to_json(),
            replace(
                manifest, generated_at=STAMP.astimezone(timezone(timedelta(hours=3)))
            ).to_json(),
        )

    def test_invalid_status_structure_and_versions(self):
        manifest = ReleaseManifest(spec(), STAMP, inventory())
        for changes in (
            {"status": "ACCEPTED"},
            {"models": inventory()[:-1]},
            {"models": inventory() + (inventory()[0],)},
            {"generated_at": datetime(2024, 1, 1)},
            {"verification_outcomes": {"_SUCCESS": True}},
            {"spec": replace(spec(), schema_version="v2")},
            {"spec": replace(spec(), assumption_policies=("unapproved",))},
        ):
            with self.assertRaises(ValueError):
                replace(manifest, **changes)

    def test_acceptance_requires_all_evidence(self):
        manifest = ReleaseManifest(spec(), STAMP, inventory())
        for status in (ReleaseStatus.VALIDATED, ReleaseStatus.ACCEPTED):
            with self.assertRaises(ValueError):
                replace(manifest, status=status)
            accepted = replace(
                manifest,
                status=status,
                models=inventory(True),
                verification_outcomes={c: True for c in VERIFICATION_CHECKS},
                reconciliation_metrics={"source_coverage": "passed"},
            )
            self.assertEqual(accepted.status, status)
            with self.assertRaises(ValueError):
                replace(
                    accepted,
                    verification_outcomes={
                        **accepted.verification_outcomes,
                        "foreign_key": False,
                    },
                )
            with self.assertRaises(ValueError):
                replace(accepted, reconciliation_metrics={})

    def test_partition_contract_and_empty_inventory(self):
        for partitions in (((), ()), (), ((("reporting_year", 2024),),)):
            with self.assertRaises(ValueError):
                ModelInventory("dim_date", partitions)
        for partitions in (
            ((),),
            ((("employee_key", 1),),),
            ((("reporting_year", True),),),
            ((("reporting_year", 2024),),) * 2,
        ):
            with self.assertRaises(ValueError):
                ModelInventory("fact_payroll", partitions)
        self.assertEqual(ModelInventory("fact_payroll", (), 0).partitions, ())
        with self.assertRaises(ValueError):
            ModelInventory("fact_payroll", (), 1)
        for changes in (
            {"row_count": -1},
            {"row_count": True},
            {"content_fingerprint": "bad"},
        ):
            with self.assertRaises(ValueError):
                replace(inventory()[0], **changes)

    def test_inventory_sorting(self):
        a = (("reporting_year", 2023),)
        b = (("reporting_year", 2024),)
        self.assertEqual(
            ModelInventory("fact_payroll", (b, a)).to_dict(),
            ModelInventory("fact_payroll", (a, b)).to_dict(),
        )


class VerificationTests(TestCase):
    def test_policy_matrix_never_repairs_or_overwrites(self):
        require_output_policy(RunMode.CREATE, None)
        with self.assertRaises(ExistingOutputError):
            require_output_policy(RunMode.VERIFY_EXISTING, None)
        for status in ReleaseStatus:
            require_output_policy(RunMode.VERIFY_EXISTING, status)
            with self.assertRaises(ExistingOutputError):
                require_output_policy(RunMode.CREATE, status)
        with self.assertRaises(ValueError):
            require_output_policy("create", None)

    def test_exact_selection_and_partition_set(self):
        manifest = ReleaseManifest(spec(), STAMP, inventory())
        request = VerificationRequest(manifest)
        self.assertFalse(request.allows_writes)
        self.assertEqual(set(request.required_checks), set(VERIFICATION_CHECKS))
        before = manifest.to_json()
        request.check_inventory(manifest)
        self.assertEqual(before, manifest.to_json())
        with self.assertRaises(ExistingOutputError):
            request.check_inventory(
                replace(manifest, spec=replace(spec(), silver_batch_id="other"))
            )
        with self.assertRaises(ExistingOutputError):
            request.check_inventory(
                replace(manifest, generated_at=STAMP + timedelta(seconds=1))
            )
        changed = tuple(
            replace(m, partitions=()) if m.model == "fact_payroll" else m
            for m in manifest.models
        )
        with self.assertRaises(ExistingOutputError):
            request.check_inventory(replace(manifest, models=changed))


class MalformedInventoryTests(TestCase):
    def test_malformed_structures_are_rejected(self):
        for partitions in ("", "2024", (1,), ((1,),), ((("reporting_year",),),)):
            with self.assertRaises(ValueError):
                ModelInventory("fact_payroll", partitions)
        with self.assertRaises(ValueError):
            replace(spec(), assumption_policies="abc")
        with self.assertRaises(ValueError):
            VerificationRequest(None)
