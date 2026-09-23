"""Read the single governed domain from JSON-compatible YAML, without dependencies."""

from hashlib import sha256
import json
from pathlib import Path


def absence_domain() -> tuple[str, ...]:
    """Existing YAML consumers and Gold share this exact reference source."""
    path = (
        Path(__file__).resolve().parents[2] / "reference_data" / "absence_reasons.yml"
    )
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not rows:
        raise ValueError("Governed absence reasons must be a nonempty list")
    names, codes = [], []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Malformed governed absence reason")
        name, code = row.get("absence_reason_name"), row.get("absence_reason_code")
        if any(type(v) is not str or not v or v != v.strip() for v in (name, code)):
            raise ValueError("Malformed governed absence reason")
        names.append(name)
        codes.append(code)
    if len(set(names)) != len(names) or len(set(codes)) != len(codes):
        raise ValueError("Duplicate governed absence reason")
    return tuple(sorted(names))


def absence_domain_fingerprint() -> str:
    """Formatting/ordering-independent identity of the enforced domain."""
    return sha256(
        json.dumps(absence_domain(), ensure_ascii=True, separators=(",", ":")).encode(
            "ascii"
        )
    ).hexdigest()
