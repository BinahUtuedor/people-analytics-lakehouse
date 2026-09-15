"""Pinned, typed, length-safe UTF-8 canonicalization for Gold contracts.

v1 framing: each string is its UTF-8 byte length (ASCII decimal), ':' and
its unchanged UTF-8 bytes. Records start with framed 'gold-record', 'v1',
model; then sorted field-name, logical-type, null-marker ('N'/'V') frames.
A non-null marker is followed by a framed canonical value. Integers use
base 10; decimals use schema scale without exponent or negative zero; dates
use YYYY-MM-DD; booleans use lowercase true/false. SHA-256 returns hex.
Only registered business fields are encoded, never technical metadata/paths.
This specification uses Spark 3.5-compatible primitives (encode, length,
concat, cast, date_format, when, sha2); no JSON serializer defaults or UDF
are needed by a future distributed implementation. Python is the reference
oracle in Phase 0, not a driver-side transformation implementation.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256
import json

from spark.gold.contracts import GOLD_MODELS, SERIALIZATION_VERSION, FieldContract


def frame(value: str) -> bytes:
    if type(value) is not str:
        raise TypeError("Canonical frames require strings")
    encoded = value.encode("utf-8")
    return str(len(encoded)).encode("ascii") + b":" + encoded


def canonical_value(field: FieldContract, value: object) -> str:
    """Reject lossy coercion, overflow and unsupported logical values."""
    kind = field.logical_type
    if kind == "STRING" and type(value) is str:
        return value
    if kind == "BOOLEAN" and type(value) is bool:
        return "true" if value else "false"
    if kind == "DATE" and type(value) is date:
        return value.isoformat()
    if kind in ("INT", "BIGINT") and type(value) is int:
        bits = 32 if kind == "INT" else 64
        if -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            return str(value)
    if kind.startswith("DECIMAL") and isinstance(value, (Decimal, str)):
        scale = int(kind[-2])
        try:
            with localcontext() as context:
                context.prec = 40
                number = Decimal(value)
                if not number.is_finite() or abs(number) >= Decimal(10) ** (18 - scale):
                    raise ValueError("Decimal must be finite and fit DECIMAL(18,s)")
                fixed = number.quantize(Decimal(1).scaleb(-scale))
                if fixed != number:
                    raise ValueError("Decimal would require rounding")
                return format(abs(fixed) if fixed == 0 else fixed, f".{scale}f")
        except InvalidOperation as error:
            raise ValueError("Invalid decimal") from error
    raise ValueError(f"Invalid {kind} value for {field.name}")


def canonical_record(model: str, row: Mapping[str, object]) -> bytes:
    """Encode all analytical fields, requiring even nullable fields to exist."""
    allowed = {f.name for f in GOLD_MODELS[model].fields} | {"_source_file"}
    if set(row) - allowed:
        raise ValueError(
            "Unapproved columns cannot be silently omitted from Gold hashing"
        )
    parts = [frame("gold-record"), frame(SERIALIZATION_VERSION), frame(model)]
    for field in sorted(GOLD_MODELS[model].business_fields, key=lambda f: f.name):
        value = row[field.name]
        parts.extend((frame(field.name), frame(field.logical_type)))
        if value is None:
            if not field.nullable:
                raise ValueError(f"Required field is null: {field.name}")
            parts.append(frame("N"))
        else:
            parts.extend((frame("V"), frame(canonical_value(field, value))))
    return b"".join(parts)


def record_hash(model: str, row: Mapping[str, object]) -> str:
    return sha256(canonical_record(model, row)).hexdigest()


def canonical_json(value: object) -> str:
    """Pinned contract JSON: string keys, sorted objects, ASCII escapes, no floats.

    Lists retain declared order; callers sort semantic sets before serialization.
    Dates, decimals and enums must be explicitly converted by their contracts.
    """

    def check(item):
        if item is None or type(item) in (str, int, bool):
            return
        if type(item) in (list, tuple):
            for child in item:
                check(child)
            return
        if type(item) is dict and all(type(k) is str for k in item):
            for child in item.values():
                check(child)
            return
        raise TypeError(
            "Canonical JSON accepts only explicit JSON types, without floats"
        )

    check(value)
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    )


def spark_record_hash(dataframe, model: str):
    """Spark built-in implementation of the pinned Phase 0 record bytes.

    Loops construct column expressions, never iterate over distributed records.
    Exact input types prevent lossy casts from changing the reference contract.
    """
    from pyspark.sql import functions as F

    contract = GOLD_MODELS[model]
    allowed = {field.name for field in contract.fields} | {"_source_file"}
    if (
        len(dataframe.columns) != len(set(dataframe.columns))
        or set(dataframe.columns) - allowed
    ):
        raise ValueError("Unapproved or duplicate hash input columns")

    def framed(value):
        return F.concat(
            F.length(F.encode(value, "UTF-8")).cast("string"), F.lit(":"), value
        )

    parts = [
        F.lit(
            b"".join(
                frame(v) for v in ("gold-record", SERIALIZATION_VERSION, model)
            ).decode("utf-8")
        )
    ]
    for field in sorted(contract.business_fields, key=lambda field: field.name):
        if (
            field.name not in dataframe.columns
            or dataframe.schema[field.name].dataType != field.spark_field().dataType
        ):
            raise ValueError(f"Hash input type differs from contract: {field.name}")
        column = F.col(field.name)
        value = (
            F.date_format(column, "yyyy-MM-dd")
            if field.logical_type == "DATE"
            else column.cast("string")
        )
        null_value = (
            F.lit(frame("N").decode())
            if field.nullable
            else F.raise_error(F.lit(f"Required Gold hash field is null: {field.name}"))
        )
        parts.extend(
            (
                F.lit((frame(field.name) + frame(field.logical_type)).decode()),
                F.when(column.isNull(), null_value).otherwise(
                    F.concat(F.lit("1:V"), framed(value))
                ),
            )
        )
    return F.sha2(F.concat(*parts), 256)
