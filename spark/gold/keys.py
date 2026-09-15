"""Pure analytical keys; no timestamp, runtime, storage or source-row ordering."""

from __future__ import annotations
from datetime import date
from hashlib import sha256
from spark.gold.contracts import KEY_VERSION, SERIALIZATION_VERSION
from spark.gold.hashing import frame


def positive_source_id(value: int) -> int:
    """Validate source-retained BIGINT identities; zero is exclusively reserved."""
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError("Source ID must be a positive signed BIGINT")
    return value


def date_key(value: date | None) -> int:
    """None is explicitly unknown; only real date objects are accepted."""
    if value is None:
        return 0
    if type(value) is not date:
        raise ValueError("Date key requires a date (not datetime or text), or None")
    return value.year * 10000 + value.month * 100 + value.day


def _key(namespace: str, kind: str, *values: str) -> str:
    if type(namespace) is not str or not namespace.strip():
        raise ValueError("An explicit nonempty source namespace is required")
    return sha256(
        b"".join(
            frame(v)
            for v in (
                "gold-key",
                KEY_VERSION,
                SERIALIZATION_VERSION,
                namespace,
                kind,
                *values,
            )
        )
    ).hexdigest()


def assignment_key(namespace: str, employee_id: int, interval_start: date) -> str:
    positive_source_id(employee_id)
    if interval_start is None:
        raise ValueError("A real assignment requires an interval start")
    date_key(interval_start)
    return _key(namespace, "assignment", str(employee_id), interval_start.isoformat())


def movement_key(namespace: str, movement_type: str, source_record_id: int) -> str:
    if movement_type not in ("HIRE", "EXIT", "PROMOTION", "TRANSFER"):
        raise ValueError("Unsupported movement type")
    positive_source_id(source_record_id)
    return _key(namespace, "movement", movement_type, str(source_record_id))
